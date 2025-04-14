import os
import sys

sys.path.append("/home/nel/Coding_Projects/Techgenius_capstone_project")


current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)


from TechGenius import extract_data, transform_data, load_data2

