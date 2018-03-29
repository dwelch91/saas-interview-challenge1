from pathlib import Path
from shutil import copytree, copyfile, rmtree
from subprocess import check_output
from zipfile import ZipFile, ZIP_DEFLATED

import boto3

root_dir = Path(__file__).parent
shared_dir = root_dir / 'shared'
bin_dir = root_dir / 'bin'
ffmpeg_path = bin_dir / 'ffmpeg'
tmp = Path('/tmp')

for target, ffmpeg_flag in [('controller', False), ('worker1', True), ('worker2', True), ('worker3', False)]:
    target_dir = root_dir / target
    print(target_dir)

    target_shared_path = target_dir / 'shared'
    target_ffmpeg_path = target_dir / 'ffmpeg'

    # Clear out old stuff
    if target_shared_path.exists():
        rmtree(target_shared_path)

    if target_ffmpeg_path.exists():
        target_ffmpeg_path.unlink()

    # Copy shared/ and ffmpeg to target area
    copytree(shared_dir, target_shared_path)
    if ffmpeg_flag:
        ffmpeg_target_path = target_dir / 'ffmpeg'
        copyfile(ffmpeg_path, ffmpeg_target_path)
        ffmpeg_target_path.chmod(0o755)

    # Install any requirements
    print("Installing pip packages...")
    requirements = target_dir / 'requirements.txt'
    check_output(['pip', 'install', '-r', str(requirements), '--target', str(target_dir), '--upgrade'])

    # Create the zip file
    print("Zipping...")
    zipfile_path = tmp / (target + '.zip')
    if zipfile_path.exists():
        zipfile_path.unlink()
    archive = ZipFile(zipfile_path, 'w', ZIP_DEFLATED)
    for f in target_dir.rglob('*'):
        file_path = f.relative_to(target_dir)
        archive.write(f, file_path)
    archive.close()

    # Upload to AWS
    print("Uploading...")
    session = boto3.Session(profile_name='sandbox')
    lambda_ = session.client('lambda', region_name='us-west-2')
    with open(zipfile_path, 'rb') as f:
        result = lambda_.update_function_code(FunctionName=target,
                                              ZipFile=f.read(),
                                              Publish=True)
        print(result)

