import os
import importlib

# Get the current directory
current_directory = os.path.dirname(__file__)

# List all files in the directory
files = os.listdir(current_directory)

# Loop through the files and import Python files
for file in files:
    if file.endswith('.py') and file != '__init__.py':
        module_name = file[:-3]  # Remove the '.py' extension
        module = importlib.import_module(f'.{module_name}', package=__name__)

        # Optional: You can also add the imported module to globals
        globals()[module_name] = module