"""cookmigration

Revision ID: 110ec6566dca
Revises: 2849d9e93551
Create Date: 2018-03-12 16:54:17.399421

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '110ec6566dca'
#down_revision = '2849d9e93551'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
	op.rename_table('cookCountyTemp','cookCounty')

def downgrade():
    pass
