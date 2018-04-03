"""PierceMigration

Revision ID: 65c4237f4c27
Revises: 
Create Date: 2018-03-11 20:53:39.913520

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '65c4237f4c27'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
	op.drop_table('pierceCounty')
	op.drop_table('pierceCountySales')
	op.rename_table('pierceCountyTemp', 'pierceCounty')
	op.rename_table('pierceCountySalesTemp', 'pierceCountySales')

	op.drop_table('cookCountyTemp')
	op.drop_table('duvalCountyTemp')
	op.drop_table('duvalCountySalesTemp')
	op.drop_table('maricopaCountyTemp')

def downgrade():
    pass


