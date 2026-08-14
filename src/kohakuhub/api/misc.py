"""Utility API endpoints for Kohaku Hub."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import yaml

from kohakuhub.config import cfg
from kohakuhub.db import User, UserOrganization
from kohakuhub.logger import get_logger
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.api.operation_capabilities import (
    get_repository_operation_capabilities,
)

logger = get_logger("UTILS")

router = APIRouter()


@router.get("/version")
def get_version():
    """Get KohakuHub version and site information.

    This endpoint helps client libraries (like hfutils) detect if they're
    connecting to KohakuHub vs HuggingFace Hub.

    HuggingFace Hub returns 404 for this endpoint.
    KohakuHub returns site identification and version info.

    Returns:
        Site identification and version information
    """
    return {
        "api": "kohakuhub",
        "version": "0.0.1",
        "name": cfg.app.site_name,
    }


@router.get("/site-config")
def get_site_config():
    """Get public site configuration.

    Returns public configuration settings that affect frontend behavior.

    Returns:
        Public site configuration
    """
    return {
        "site_name": cfg.app.site_name,
        "invitation_only": cfg.auth.invitation_only,
        "require_email_verification": cfg.auth.require_email_verification,
        "capabilities": {
            "repository_operations": get_repository_operation_capabilities(),
        },
    }


class ValidateYamlPayload(BaseModel):
    """Payload for YAML validation endpoint."""

    content: str
    repo_type: str = "model"


@router.post("/validate-yaml")
def validate_yaml(body: ValidateYamlPayload):
    """Validate YAML content (e.g., model card, dataset card).

    Args:
        body: Validation payload with YAML content

    Returns:
        Validation result
    """
    try:
        yaml.safe_load(body.content)
    except Exception as e:
        return {"valid": False}

    return {"valid": True}


@router.get("/whoami-v2")
def whoami_v2(user: User | None = Depends(get_optional_user)):
    """Get current user information (HuggingFace compatible).

    Matches HuggingFace Hub /api/whoami-v2 endpoint format.
    Returns user info if authenticated, 401 if not.
    """
    if not user:
        raise HTTPException(401, detail="Invalid user token")

    # Get user's organizations (organizations are User objects with is_org=True)
    user_orgs = (
        UserOrganization.select(UserOrganization, User)
        .join(User, on=(UserOrganization.organization == User.id))
        .where(UserOrganization.user == user)
    )

    orgs_list = []
    for uo in user_orgs:
        orgs_list.append(
            {
                "name": uo.organization.username,
                "fullname": uo.organization.username,
                "roleInOrg": uo.role,
            }
        )

    return {
        "type": "user",
        "id": str(user.id),
        "name": user.username,
        "fullname": user.username,
        "email": user.email,
        "emailVerified": user.email_verified,
        "canPay": False,
        "isPro": False,
        "orgs": orgs_list,
        "auth": {
            "type": "access_token",
            "accessToken": {"displayName": "Auto-generated token", "role": "write"},
        },
        # KohakuHub-specific fields
        "site": {
            "name": cfg.app.site_name,  # Configurable site name
            "api": "kohakuhub",  # Hardcoded API identifier
            "version": "0.0.1",  # Hardcoded version
        },
    }
