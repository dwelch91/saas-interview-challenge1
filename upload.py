from pathlib import Path
from shutil import copytree

root_dir = Path(__file__).parent
shared_dir = root_dir / 'shared'
bin_dir = root_dir / 'bin'
ffmpeg = bin_dir / 'ffmpeg'

for target in ['controller', 'worker1', 'worker2', 'worker3']:
    target_dir = root_dir / target
    print(target_dir)
    copytree(shared_dir, target_dir / 'shared')




