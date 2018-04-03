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
	op.drop_table('cookcounty')
	op.rename_table('cookCountyTemp','cookcounty')

	op.drop_table('pierceCountyTemp')
	op.drop_table('pierceCountySalesTemp')
	op.drop_table('duvalCountyTemp')
	op.drop_table('duvalCountySalesTemp')
	op.drop_table('maricopaCountyTemp')


def downgrade():
    pass
