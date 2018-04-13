"""maricopamigration

Revision ID: 9dcae9ce3303
Revises: 110ec6566dca
Create Date: 2018-04-03 10:49:34.940253

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9dcae9ce3303'
#down_revision = '110ec6566dca'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('maricopaCounty')
    op.rename_table('maricopaCountyTemp','maricopaCounty')

def downgrade():
    pass
