from enum import Enum
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_OBJECT_STORAGE_BACKEND = BlockedStatus("Missing Object Storage backend")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    MISSING_KAFKA = BlockedStatus("Missing kafka relation")
    MISSING_INTEGRATION_HUB = BlockedStatus("Missing integration hub relation")
    INVALID_NAMESPACE = BlockedStatus("Invalid config option: namespace")
    INVALID_SERVICE_ACCOUNT = BlockedStatus("Invalid config option: service-account")
    INSUFFICIENT_CLUSTER_PERMISSIONS = BlockedStatus(
        "Insufficient cluster permissions. Try: juju trust --scope=cluster <app-name>"
    )

    ACTIVE = ActiveStatus("")
