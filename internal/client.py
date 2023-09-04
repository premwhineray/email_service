import hashlib
import re

from imap_tools import OR, MailBox, MailMessage
from inscriptis import get_text

from concurrent.futures import ThreadPoolExecutor
from .models import Account, Attachment, Email, EmailAttachment
from .database import Database

class Client:
    def __init__(self, db: Database, host: str, username: str, password: str) -> None:
        self.host: str = host
        self.username: str = username
        self.password: str = password
        self.db: Database = db

        self.account: Account = self.db.get_account(username=username)
    
    def _convert_MailMessage_to_Email_and_EmailAttachments(self, folder: str, message: MailMessage) -> tuple[Email, list[EmailAttachment]]:
        to_addresses = ",".join([email.email.lower() for email in message.to_values])

        subject = message.subject.replace("\n", " ").strip()

        email = Email(
            hash = self._generate_email_id(message=message),
            account_id = self.account.id,
            datetime = message.date,
            mailbox = folder,
            mailbox_id = (message.uid or ""),
            from_address = message.from_.lower(),
            to_addresses = to_addresses,
            subject = subject,
            thread = self._convert_subject_to_thread(subject=subject),
            text = self._get_text(message=message),
            analytics_version = 0,
            analytics_data = {}
        )

        emailAttachments: list[EmailAttachment] = []

        for message_attachment in message.attachments:
             # Exclude certain file types
            if message_attachment.content_type in ["application/ics", "text/calendar", "text/calender"]:
                continue

            if message_attachment.content_type in ["message/rfc822", "message/delivery-status", "text/html", "text/plain"]:
                continue

            if "image/" in message_attachment.content_type:
                continue

            attachment = Attachment(
                hash = hashlib.sha256(message_attachment.payload).hexdigest(),
                content = message_attachment.payload
            )

            emailAttachment = EmailAttachment(
                account_id = self.account.id,
                email_hash = email.hash,
                attachment_hash = attachment.hash,
                filename = message_attachment.filename,
                content_type = message_attachment.content_type,
                attachment = attachment
            )

            emailAttachments.append(emailAttachment)
        
        return email, emailAttachments

    def _generate_email_id(self, message: MailMessage) -> str:
        return hashlib.sha256(f"{self.account.username}_{message.date_str}{message.from_}{message.subject}".encode()).hexdigest()

    def _convert_subject_to_thread(self, subject: str) -> str:
        thread = subject.lower()

        # Strip out all Re: and Fwd: from the subject
        thread = thread.replace("re: ", "")
        thread = thread.replace("fwd: ", "")

        thread = re.sub(r':\s+\w+\s+\d{1,2},\s+\d{4}', '', thread)

        # Now strip all Emoji Characters, leaving only ASCII
        thread = thread.encode("ascii", "ignore").decode()

        # Now join multiple lines into one
        thread = " ".join(thread.splitlines())

        # Now replace multiple spaces with a single space
        thread = re.sub(r'\s+', ' ', thread)
    
        return thread
    
    def _get_text(self, message: MailMessage) -> str:
        text: str = ""
        if message.html != "":
            text = get_text(html_content=message.html)
        elif message.text != "":
            text = message.text
        
        # Trim whitespace from the beginning and end of each line
        text = "\n".join([line.strip() for line in text.split("\n")])   

        # Now strip out all the empty lines
        text = "\n".join([line for line in text.split("\n") if line != ""])

        # Strip out any lines that start with RFC 5322 Message Headers, e.g. "From: ", "To: ", "Subject: ", etc. or a quoted reply
        text = "\n".join([line for line in text.split("\n") if not line.upper().startswith(("FROM: ", "TO: ", "SUBJECT: ", "DATE: ", "CC: ", "BCC: ", "> ", ">> "))]) 

        # Now remove content that does not provide any value
        text = text.replace("________________________________", "")
        text = text.replace("This message was sent from a notification-only email address that does not accept incoming email. Please do not reply to this message.", "")   
        text = text.replace("This is an automated message. Please do not reply to this email.", "")   

        return text
    
    def _get_folders(self) -> list[str]:
        with MailBox(self.host).login(self.username, self.password) as mailbox:
            folders : list[str] = []
            for folder in mailbox.folder.list():
                if folder.name in ["Outbox", "Notes", "Junk", "Drafts", "Trash"]:
                    continue

                if folder.name.startswith("Sync Issues") or folder.name.startswith("Deleted"):  
                    continue

                folders.append(folder.name)

            return folders
    
    def _get_all_messages_from_folder(self, folder: str, headers_only: bool = False) -> list[MailMessage]:
        with MailBox(self.host).login(self.username, self.password, initial_folder=folder) as mailbox:
            messages = [msg for msg in mailbox.fetch(bulk=True, mark_seen=False, headers_only=headers_only)]
            return messages
    
    def _get_specific_messages_from_folder(self, folder: str, uids: list[str]) -> list[MailMessage]:
        with MailBox(self.host).login(self.username, self.password, initial_folder=folder) as mailbox:
            messages = [msg for msg in mailbox.fetch(criteria=OR(uid=uids), bulk=True, mark_seen=False)]
            return messages
    
    def _thread_fetch_emails_from_folder(self, folder):
        print(f"{self.account.username}: Querying folder: {folder}")
        messages = self._get_all_messages_from_folder(folder=folder, headers_only=True)
        print(f"{self.account.username}: Found {len(messages)} messages in folder: {folder}")

        missing_emails: list[str] = []

        for message in messages:
            try:
                email, emailAttachments = self._convert_MailMessage_to_Email_and_EmailAttachments(folder=folder, message=message)

                # First check if the email already exists
                if self.db.check_email_exists(email):
                    continue

            except BaseException as e:
                print(e)
                continue
            
            missing_emails.append(email.mailbox_id)
        
        if len(missing_emails) == 0:
            return
        
        print(f"{self.account.username}: Fetching {len(missing_emails)} missing emails from folder: {folder}")

        # Now fetch the full email
        messages = self._get_specific_messages_from_folder(folder=folder, uids=missing_emails)

        for message in messages:
            email, emailAttachments = self._convert_MailMessage_to_Email_and_EmailAttachments(folder=folder, message=message)
            
            self.db.add_email(email)
            
            for emailAttachment in emailAttachments:
                self.db.add_emailAttachment(emailAttachment)
        
        print(f"{self.account.username}: Finished Fetching Emails from folder: {folder}")

    def fetch_emails(self, folder: str = "ALL") -> None:
        if folder == "ALL":
            folders = self._get_folders()
        else:
            folders = [folder]

        
        # Lets divide and conquer with concurrent.futures
        with ThreadPoolExecutor(max_workers=14) as executor:
            # Divide each folder into its own thread up to the max_workers
            # executor.map will wait until all threads are finished

            executor.map(self._thread_fetch_emails_from_folder, folders)

        print(f"{self.account.username}: Finished Fetching Emails")