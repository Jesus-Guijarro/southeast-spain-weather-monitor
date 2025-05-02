import shutil
import subprocess
import os
from datetime import datetime
from pathlib import Path
import configparser
import platform

BASE_DIR       = Path(__file__).resolve().parent
CONFIG_PATH    = BASE_DIR / 'config.ini'
BACKUP_ROOT    = BASE_DIR / 'backup'
LOG_FILE       = BASE_DIR / 'logs' / 'pipeline.log'
KEEP_LAST      = 10  # number of ZIPs to keep

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_cfg = config['database']
DB_TYPE       = 'postgres'
DB_NAME       = db_cfg.get('dbname')
DB_USER       = db_cfg.get('user')
DB_PASSWORD   = db_cfg.get('password')
DB_HOST       = db_cfg.get('host', 'localhost')
DB_PORT       = db_cfg.get('port', '5432')


if platform.system() == 'Windows':
    PG_DUMP_EXE = r'C:\Program Files\PostgreSQL\16\bin\pg_dump.exe'
else:
    PG_DUMP_EXE = 'pg_dump'

def ensure_dirs():
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)

def timestamp() -> str:
    return datetime.now().strftime('%Y-%m-%d')

def dump_database(session_dir: Path):
    dump_path = session_dir / f'{DB_NAME}.sql'
    cmd = [
        PG_DUMP_EXE,
        '-w',
        '-U', DB_USER,
        '-h', DB_HOST,
        '-p', DB_PORT,
        '-f', str(dump_path),
        DB_NAME,
    ]
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_PASSWORD
    subprocess.run(cmd, check=True, env=env)


def copy_log(session_dir: Path):
    shutil.copy2(LOG_FILE, session_dir / f'{LOG_FILE.stem}_{timestamp()}{LOG_FILE.suffix}')


def compress_and_cleanup(session_dir: Path) -> Path:
    zip_name = f'backup_data_and_logs_{session_dir.name}.zip'
    zip_path = BACKUP_ROOT / zip_name
    shutil.make_archive(str(zip_path.with_suffix('')), 'zip', session_dir)
    shutil.rmtree(session_dir)
    return zip_path


def rotate_log():
    LOG_FILE.write_text('')


def purge_old_backups():
    archives = sorted(BACKUP_ROOT.glob('weather_backup_*.zip'))
    for old in archives[:-KEEP_LAST]:
        old.unlink()


def main():
    ensure_dirs()
    session = BACKUP_ROOT / timestamp()
    session.mkdir()

    dump_database(session)
    copy_log(session)
    zip_file = compress_and_cleanup(session)
    rotate_log()
    purge_old_backups()
    print(f'Backup completed: {zip_file}')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'Error: {exc}')
        raise
