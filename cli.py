import multiprocessing
import shutil

import click
import os

from icfs.filesystem.filesystem import FileSystem


@click.group()
def cli():
    pass
    # click.echo("Integrated Cloud File System")


@cli.command(name="mount")
@click.argument('mount_location')
@click.option('-i', '--init', type=bool, default=False, flag_value=True,
              help='Initialize ICFS')
@click.option('-a', '--no-accounts', type=int, default="3", flag_value=1,
              help='Number of Accounts')
def icfs_mount(mount_location, init, no_accounts):
    icfs_creds_dir = os.path.expanduser('~/.icfs/creds')
    icfs_meta_dir = os.path.expanduser('~/.icfs/meta')
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
        if no_accounts == 1:
            fs.add_account()
    click.echo("Mounting at location [{}]...".format(mount_location))
    click.echo("Starting...")
    p = multiprocessing.Process(target=fs.start,
                                name='icfs')
    p.start()


def init_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    return path