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
    op.drop_table('piercecounty')
    op.drop_table('piercecountysales')
    op.rename_table('pierceCountyTemp', 'piercecounty')
    op.rename_table('pierceCountySalesTemp', 'piercecountysales')
    op.drop_table("duvalcountytemp")
    op.drop_table("duvalCountySalesTemp")
def downgrade():
    pass


