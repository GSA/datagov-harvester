import click
from flask import Blueprint

from database.interface import HarvesterDBInterface

user = Blueprint("user", __name__)

db = HarvesterDBInterface()


# CLI commands
## User management
@user.cli.command("add")
@click.argument("email")
@click.option("--name", default="", help="Name of the user")
def add_user(email, name):
    """add new user with .gov email."""

    email = email.lower()
    usr_data = {"email": email}
    if name:
        usr_data["name"] = name

    success, message = db.add_user(usr_data)
    if success:
        print("User added successfully!")
    else:
        print("Error:", message)


@user.cli.command("list")
def list_users():
    """List all users' emails."""
    users = db.list_users()
    if users:
        for user in users:
            print(user.email)
    else:
        print("No users found.")


@user.cli.command("remove")
@click.argument("email")
def remove_user(email):
    """Remove a user with the given EMAIL."""
    email = email.lower()
    if db.remove_user(email):
        print(f"Removed user with email: {email}")
    else:
        print("Failed to remove user or user not found.")
