
CREATE EXTERNAL SCHEMA amzreviews 
from data catalog
database 'amzreviews'
iam_role 'arn:aws:iam::983491514089:role/myproject_redshiftrole'
CREATE EXTERNAL database IF NOT EXISTS;
