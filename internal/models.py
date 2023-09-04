from dataclasses import dataclass
from datetime import datetime
from typing import Any

@dataclass
class Account:
    id: int
    username: str
    last_synced: datetime

@dataclass
class Email:
    hash: str
    account_id: int
    datetime: datetime
    mailbox: str
    mailbox_id: str
    from_address: str
    to_addresses: str
    subject: str
    thread: str
    text: str
    analytics_version: int
    analytics_data: dict[str, Any]

@dataclass
class Attachment:
    hash: str
    content: bytes

@dataclass
class EmailAttachment:
    account_id: int
    email_hash: str
    attachment_hash: str
    filename: str
    content_type: str
    attachment: Attachment