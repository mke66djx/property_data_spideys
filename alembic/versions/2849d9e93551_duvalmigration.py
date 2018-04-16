"""DuvalMigration

Revision ID: 2849d9e93551
Revises: 65c4237f4c27
Create Date: 2018-03-11 20:55:00.284702

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2849d9e93551'
#down_revision = '65c4237f4c27'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
	op.rename_table('duvalCountyTemp', 'duvalCounty')

def downgrade():
    pass
