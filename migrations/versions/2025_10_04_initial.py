"""initial

Revision ID: 3996104ec99e
Revises:
Create Date: 2025-10-04 14:00:19.054838

"""

from alembic import op
import sqlalchemy as sa
from backend.database.queries import (
    CREATE_CHECK_CONFLICTS_FUNCTION_QUERY,
    DROP_CHECK_CONFLICTS_FUNCTION_QUERY
)


# revision identifiers, used by Alembic.
revision = "3996104ec99e"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(CREATE_CHECK_CONFLICTS_FUNCTION_QUERY)
    op.create_table(
        "users",
        sa.Column("doc_num", sa.String(length=16), nullable=False),
        sa.Column("last_name", sa.String(length=20), nullable=False),
        sa.Column("first_name", sa.String(length=20), nullable=False),
        sa.Column("second_name", sa.String(length=20), nullable=True),
        sa.Column("second_name1", sa.String(length=4), nullable=True),
        sa.Column("birth_date", sa.Date(), nullable=True),
        sa.Column("is_man", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("doc_num"),
    )


def downgrade() -> None:
    op.drop_table("users")
    op.execute(DROP_CHECK_CONFLICTS_FUNCTION_QUERY)
