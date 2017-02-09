from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
import settings


def need_restrict(key, bucket_config):
    for folder in bucket_config['folders']:
        if key.key.startswith(folder):
            return key.key != folder
    return False

removed_uri = ['http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
               'http://acs.amazonaws.com/groups/global/AllUsers']

for bucket_config in settings.buckets:
    s3 = S3Connection(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        host=bucket_config['host']
    )

    try:
        bucket = s3.get_bucket(bucket_config['name'])
    except S3ResponseError as e:
        print(e)
        continue

    for key in bucket.list():
        if not need_restrict(key, bucket_config):
            continue

        print('Change access for %s' % key.key)
        print('Grants count before: %d' % len(acl.acl.grants))

        new_grants = []
        acl = key.get_acl()
        for grant in acl.acl.grants:
            if grant.uri not in removed_uri:
                new_grants.append(grant)
        acl.acl.grants = new_grants
        key.set_acl(acl)

        print('Grants count after: %d' % len(new_grants))
