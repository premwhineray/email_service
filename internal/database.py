import sqlite3
from .models import Account, Attachment, Email, EmailAttachment
from datetime import datetime
import json

from typing import Tuple

class Database:
    def __init__(self, db: sqlite3.Connection) -> None:
        self.db: sqlite3.Connection = db
        self._create_tables()

    # Create Tables
    def _create_tables(self) -> None:
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY,
                username TEXT,
                last_synced TEXT
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                hash TEXT PRIMARY KEY,
                account_id INTEGER,
                datetime TEXT,
                mailbox TEXT,
                mailbox_id TEXT,
                from_address TEXT,
                to_addresses TEXT,
                subject TEXT,
                thread TEXT,
                text TEXT,
                analytics_version INTEGER,
                analytics_data TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS email_attachments (
                account_id INTEGER,
                email_hash TEXT,
                attachment_hash TEXT,
                filename TEXT,
                content_type TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(id),
                FOREIGN KEY(email_hash) REFERENCES emails(hash),
                FOREIGN KEY(attachment_hash) REFERENCES attachments(hash)
                PRIMARY KEY(account_id, email_hash, attachment_hash)
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS attachments (
                hash TEXT PRIMARY KEY,
                content BLOB
            )
        """)

        self.db.commit()

    def check_email_attachment_exists(self, emailAttachment: EmailAttachment) -> bool:
        cursor = self.db.execute("""
            SELECT attachment_hash
            FROM email_attachments
            WHERE account_id = ? AND email_hash = ? AND attachment_hash = ?
        """, (emailAttachment.account_id, emailAttachment.email_hash, emailAttachment.attachment_hash))

        return cursor.fetchone() is not None
    
    def check_attachment_exists(self, attachment: Attachment) -> bool:
        cursor = self.db.execute("""
            SELECT hash
            FROM attachments
            WHERE hash = ?
        """, (attachment.hash,))

        return cursor.fetchone() is not None

    def check_email_exists(self, email: Email) -> bool:
        cursor = self.db.execute("""
            SELECT hash
            FROM emails
            WHERE hash = ?
        """, (email.hash,))

        return cursor.fetchone() is not None
    
    def get_account(self, username: str) -> Account:
        # Check if account exists, if not, create it
        cursor = self.db.execute("""
            SELECT id, username, last_synced
            FROM accounts
            WHERE username = ?
        """, (username,))

        row = cursor.fetchone()

        if row is None:
            cursor = self.db.execute("""
                INSERT INTO accounts (username, last_synced) VALUES (?, ?)
            """, (username, datetime.now()))

            self.db.commit()

            return Account(
                id=cursor.lastrowid, # type: ignore
                username=username,
                last_synced=datetime.fromisoformat("1970-01-01T00:00:00")
            )
        else:
            return Account(
                id=row[0],
                username=row[1],
                last_synced=datetime.fromisoformat(row[2])
            )

    def set_account_last_synced(self, account: Account) -> None:
        self.db.execute("""
            UPDATE accounts SET last_synced = ? WHERE id = ?
        """, (datetime.now(), account.id))

        self.db.commit()    
    
    def _add_attachment(self, attachment: Attachment) -> None:
        self.db.execute("""
            INSERT INTO attachments (hash, content) VALUES (?, ?)
        """, (attachment.hash, attachment.content))

        self.db.commit()

    def add_emailAttachment(self, emailAttachment: EmailAttachment) -> None:  
        # We want to add the attachment to the database if it doesn't exist
        # Otherwise, we want to not duplicate the attachment
        # but still add it to the email_attachments table as a link

        if not self.check_attachment_exists(emailAttachment.attachment):
            self._add_attachment(emailAttachment.attachment)

        if not self.check_email_attachment_exists(emailAttachment):
            self.db.execute("""
                INSERT INTO email_attachments (account_id, email_hash, attachment_hash, filename, content_type)
                VALUES (?, ?, ?, ?, ?)
            """, (emailAttachment.account_id, emailAttachment.email_hash, emailAttachment.attachment_hash, emailAttachment.filename, emailAttachment.content_type))  

            self.db.commit()

    def add_email(self, email: Email) -> None:
        self.db.execute("""
            INSERT INTO emails (
                hash,
                account_id,
                datetime,
                mailbox,
                mailbox_id,
                from_address,
                to_addresses,
                subject,
                thread,
                text,
                analytics_version,
                analytics_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            email.hash,
            email.account_id,
            email.datetime,
            email.mailbox,
            email.mailbox_id,
            email.from_address,
            email.to_addresses,
            email.subject,
            email.thread,
            email.text,
            email.analytics_version,
            json.dumps(email.analytics_data)
        ))

        self.db.commit()
    
    # Different Getters for different purposes
    def get_emails(self, account: Account) -> list[Email]:
        cursor = self.db.execute("""
            SELECT hash, account_id, datetime, mailbox, mailbox_id, from_address, to_addresses, subject, thread, text, analytics_version, analytics_data
            FROM emails
            WHERE account_id = ?
            ORDER BY datetime DESC
        """, (account.id,))

        return [Email(
            hash=row[0],
            account_id=row[1],
            datetime=datetime.fromisoformat(row[2]),
            mailbox=row[3],
            mailbox_id=row[4],
            from_address=row[5],
            to_addresses=row[6],
            subject=row[7],
            thread=row[8],
            text=row[9],
            analytics_version=row[10],
            analytics_data=json.loads(row[11])
        ) for row in cursor.fetchall()]
    
    def get_emails_with_analytics_version_less_than(self, account: Account, version: int) -> list[Email]:
        cursor = self.db.execute("""
            SELECT hash, account_id, datetime, mailbox, mailbox_id, from_address, to_addresses, subject, thread, text, analytics_version, analytics_data
            FROM emails
            WHERE account_id = ? AND analytics_version < ?
            ORDER BY datetime DESC
        """, (account.id, version))

        return [Email(
            hash=row[0],
            account_id=row[1],
            datetime=datetime.fromisoformat(row[2]),
            mailbox=row[3],
            mailbox_id=row[4],
            from_address=row[5],
            to_addresses=row[6],
            subject=row[7],
            thread=row[8],
            text=row[9],
            analytics_version=row[10],
            analytics_data=json.loads(row[11])
        ) for row in cursor.fetchall()]
    
    def update_email_analytics(self, email: Email, version: int, data: dict) -> None:
        self.db.execute("""
            UPDATE emails SET analytics_version = ?, analytics_data = ? WHERE hash = ?
        """, (version, json.dumps(data), email.hash))

        self.db.commit()
    
    def get_unique_email_threads(self, account: Account) -> list[Tuple[str, int]]:
        # For a given account, get all unique email threads
        # These threads are to be sorted by datetime, where the most recent thread is the first result
        # We also want to get the number of emails in each thread

        cursor = self.db.execute("""
            SELECT thread, COUNT(*) FROM emails WHERE account_id = ? GROUP BY thread ORDER BY datetime DESC
        """, (account.id,))

        return cursor.fetchall()
    
    def get_emails_in_thread(self, account: Account, thread: str) -> list[Email]:
        # Where first email in thread is the latest email

        cursor = self.db.execute("""
            SELECT hash, account_id, datetime, mailbox, mailbox_id, from_address, to_addresses, subject, thread, text, analytics_version, analytics_data
            FROM emails
            WHERE account_id = ? AND thread = ?
            ORDER BY datetime DESC
        """, (account.id, thread))

        return [Email(
            hash=row[0],
            account_id=row[1],
            datetime=datetime.fromisoformat(row[2]),
            mailbox=row[3],
            mailbox_id=row[4],
            from_address=row[5],
            to_addresses=row[6],
            subject=row[7],
            thread=row[8],
            text=row[9],
            analytics_version=row[10],
            analytics_data=json.loads(row[11])
        ) for row in cursor.fetchall()]
    
    def get_list_of_accounts(self) -> list[Account]:
        cursor = self.db.execute("""
            SELECT id, username, last_synced FROM accounts
        """)

        return [Account(
            id=row[0],
            username=row[1],
            last_synced=datetime.fromisoformat(row[2])
        ) for row in cursor.fetchall()]