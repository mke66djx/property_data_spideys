"""'duval_full_sales_migration'

Revision ID: d568fb36db9e
Revises: 9dcae9ce3303
Create Date: 2018-04-16 22:40:11.543123

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd568fb36db9e'
#down_revision = '9dcae9ce3303'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.rename_table('duvalCountySalesTemp', 'duvalCountySales')

def downgrade():
    pass