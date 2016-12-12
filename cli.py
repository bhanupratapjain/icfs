import json
import multiprocessing
import os
import shutil

import click

from icfs.cloudapi.cloud import Cloud
from icfs.filesystem import constants
from icfs.filesystem.filesystem import FileSystem
from icfs.global_constants import PROJECT_ROOT

icfs_creds_dir = os.path.expanduser('~/.icfs/creds')
icfs_meta_dir = os.path.expanduser('~/.icfs/meta')


@click.group()
def cli():
    pass
    # click.echo("Integrated Cloud File System")


@cli.command(name="mount")
@click.argument('mount_location')
@click.option('-i', '--init', type=bool, default=False, flag_value=True,
              help='Initialize ICFS')
@click.option('-a', '--no-accounts', type=int, default="3",
              help='Number of Accounts')
def icfs_mount(mount_location, init, no_accounts):
    if init:
        click.echo("Initializing ICFS...")
        if os.path.exists(icfs_creds_dir):
            shutil.rmtree(icfs_creds_dir, ignore_errors=True)
        if os.path.exists(icfs_meta_dir):
            shutil.rmtree(icfs_meta_dir, ignore_errors=True)
        fs = FileSystem(mount_location, icfs_creds_dir, icfs_meta_dir)
        click.echo("Adding total [{}] accounts...".format(no_accounts))
        for x in range(no_accounts):
            click.echo("Adding Account [{}]...".format(x + 1))
            fs.add_account()
    else:
        click.echo("Restoring ICFS...")
        fs = FileSystem(mount_location, icfs_creds_dir, icfs_meta_dir)
        # if no_accounts == 1:
        #     fs.add_account()
    click.echo("Mounting at location [{}]...".format(mount_location))
    click.echo("Starting...")
    p = multiprocessing.Process(target=fs.start,
                                name='icfs')
    p.start()


def hconvert(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def load_accounts(cloud):
    data = {}
    accounts = []
    with open(os.path.join(icfs_creds_dir, constants.CLOUD_ACCOUNTS_FILE_NAME),
              "r") as af:
        data = json.load(af)
    for account_id in data['accounts']:
        accounts.append(account_id)
        cloud.restore_gdrive(account_id)
    return accounts


@cli.command(name="stat")
def stats():
    cloud = Cloud(os.path.join(PROJECT_ROOT, "gdirve_settings.yaml"),
                  icfs_meta_dir, icfs_creds_dir)
    t_bytes = 0
    t_u_bytes = 0
    accounts = load_accounts(cloud)
    for acc in accounts:
        ab = cloud.about(acc)
        t_bytes += long(ab["total_quota"])
        t_u_bytes += long(ab["used_quota"])

    print "Total Storage: ", t_bytes
    print "Free Storage: ", t_bytes - t_u_bytes


def init_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    return path


if __name__ == "__main__":
    cli()
