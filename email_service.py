import os
import sqlite3

from dotenv import load_dotenv

from internal.client import Client
from internal.database import Database
from internal.analyse_emails import analyse_emails

def fetch_all_emails(db: Database, hosts: list[str], users: list[str], passwords: list[str]):
    for idx, host in enumerate(hosts):
        user = users[idx]
        password = passwords[idx]

        print(f"Fetching emails from {user}")
    
        client = Client(db=db, host=host, username=user, password=password)
        client.fetch_emails("ALL")

if __name__ == "__main__":
    load_dotenv()

    env_hosts: str | None = os.getenv("IMAP_HOST")
    env_users = os.getenv("IMAP_USER")
    env_passwords = os.getenv("IMAP_PASS")

    if env_hosts is None:
        raise ValueError("IMAP_HOST environment variable not set")

    if env_users is None:
        raise ValueError("IMAP_USER environment variable not set")
    
    if env_passwords is None:
        raise ValueError("IMAP_PASSWORD environment variable not set")
    
    hosts = env_hosts.split(",")
    users = env_users.split(",")
    passwords = env_passwords.split(",")
    
    os.makedirs("data/database", exist_ok=True)
    conn = sqlite3.connect("data/database/messages.db", isolation_level=None, check_same_thread=False)
    db = Database(db=conn)

    # Provide a menu of options for actions
    print("What would you like to do?")
    print("1. Fetch all emails")
    print("2. Analyse emails")

    choice = input("Choice: ")

    if choice == "1":
        fetch_all_emails(db=db, hosts=hosts, users=users, passwords=passwords)
    elif choice == "2":
        analyse_emails(db=db)
