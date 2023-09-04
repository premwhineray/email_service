from .database import Database
from .models import Account, Attachment, Email, EmailAttachment

def analyse_emails(db: Database):
    accounts: list[Account] = db.get_list_of_accounts()

    for account in accounts:
        emails: list[Email] = db.get_emails(account=account)

        for email in emails:
            print(f"{account.username} - {email.datetime} - {email.mailbox} - {email.subject}")