"""pierceDescTableRename

Revision ID: 8cfcecac3ca0
Revises: 
Create Date: 2018-03-08 19:00:46.244864

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '8cfcecac3ca0'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
	op.drop_table('piercecounty')
	op.drop_table('piercecountysales')
	op.rename_table('pierceCountyTemp', 'piercecounty')
	op.rename_table('pierceCountySalesTemp', 'piercecountysales')
	op.drop_table('pierceCountyTemp')
	op.drop_table('pierceCountySalesTemp')
	print("FUcking doing it")
def downgrade():
    pass
