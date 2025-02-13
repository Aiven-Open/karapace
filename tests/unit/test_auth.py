"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import re

from karapace.core.auth import ACLAuthorizer, ACLEntry, HashAlgorithm, Operation, User, hash_password


def test_empty_acl_authorizer() -> None:
    authorizer = ACLAuthorizer()
    admin_password_hash = hash_password(algorithm=HashAlgorithm.SHA256, salt="salt", plaintext_password="password")
    assert False is authorizer.check_authorization(
        user=User(username="admin", algorithm=HashAlgorithm.SHA256, salt="salt", password_hash=admin_password_hash),
        operation=Operation.Read,
        resource="Subject:*",
    )


def test_acl_authorizer() -> None:
    admin_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="admin_password",
    )
    read_user_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="read_password",
    )
    write_user_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="write_password",
    )
    readwrite_user_password_hash = hash_password(
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        plaintext_password="readwrite_password",
    )

    admin_user = User(
        username="admin",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=admin_password_hash,
    )
    read_user = User(
        username="read",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=read_user_password_hash,
    )
    write_user = User(
        username="write",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=write_user_password_hash,
    )
    readwrite_user = User(
        username="readwrite",
        algorithm=HashAlgorithm.SHA256,
        salt="salt",
        password_hash=readwrite_user_password_hash,
    )

    authorizer = ACLAuthorizer(
        user_db={
            "admin": admin_user,
            "read": read_user,
            "write": write_user,
            "readwrite": readwrite_user,
        },
        permissions=[
            ACLEntry("admin", Operation.Read, re.compile("Subject:*")),
            ACLEntry("admin", Operation.Write, re.compile("Subject:*")),
            ACLEntry("admin", Operation.Read, re.compile("Config:*")),
            ACLEntry("admin", Operation.Write, re.compile("Config:*")),
            ACLEntry("read", Operation.Read, re.compile("Subject:read_subject")),
            ACLEntry("write", Operation.Write, re.compile("Subject:write_subject")),
            ACLEntry("readwrite", Operation.Read, re.compile("Subject:readwrite_subject")),
            ACLEntry("readwrite", Operation.Write, re.compile("Subject:readwrite_subject")),
        ],
    )

    assert True is authorizer.check_authorization(
        user=admin_user,
        operation=Operation.Read,
        resource="Subject:any_subject",
    )
    assert True is authorizer.check_authorization(
        user=admin_user,
        operation=Operation.Read,
        resource="Config:any_config",
    )
    assert True is authorizer.check_authorization_any(
        user=admin_user,
        operation=Operation.Read,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )

    assert True is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Read,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Read,
        resource="Subject:any_subject",
    )
    assert False is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Write,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization(
        user=read_user,
        operation=Operation.Write,
        resource="Subject:write_subject",
    )
    assert False is authorizer.check_authorization_any(
        user=read_user,
        operation=Operation.Read,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )
    assert True is authorizer.check_authorization_any(
        user=read_user,
        operation=Operation.Read,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
            "Subject:read_subject",
        ],
    )

    assert True is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Write,
        resource="Subject:write_subject",
    )
    assert False is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Write,
        resource="Subject:any_subject",
    )
    assert False is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Write,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization(
        user=write_user,
        operation=Operation.Read,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization_any(
        user=write_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )
    assert True is authorizer.check_authorization_any(
        user=write_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
            "Subject:write_subject",
        ],
    )

    assert True is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Write,
        resource="Subject:readwrite_subject",
    )
    assert True is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Read,
        resource="Subject:readwrite_subject",
    )
    assert False is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Write,
        resource="Subject:any_subject",
    )
    assert False is authorizer.check_authorization(
        user=readwrite_user,
        operation=Operation.Write,
        resource="Subject:read_subject",
    )
    assert False is authorizer.check_authorization_any(
        user=readwrite_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
        ],
    )
    assert True is authorizer.check_authorization_any(
        user=readwrite_user,
        operation=Operation.Write,
        resources=[
            "Config:any_config",
            "Subject:any_subject",
            "Unknown:resource",
            "Subject:readwrite_subject",
        ],
    )
