import os

# Define the folder path here
folder_path = "results-3dn"
output_file = "aggregated_code.txt"

# Exceptions
excluded_folders = ["__pycache__", "venv", ".git"]  # skip these folders
excluded_files = ["vit_qchunk_topk.yaml", "vit_qchunk.yaml"]  # skip these files

with open(output_file, "w", encoding="utf-8") as out:
    for root, dirs, files in os.walk(folder_path):
        # Remove excluded folders from traversal
        dirs[:] = [d for d in dirs if d not in excluded_folders]

        for file in files:
            if file not in excluded_files:  # Exclude specific files
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, folder_path)

                out.write(f"{relative_path}\n")
                out.write("=" * 80 + "\n")  # separator line

                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        out.write(f.read())
                except Exception as e:
                    out.write(f"Error reading file: {e}")

                out.write("\n\n")  # space between files

print(f"All files have been written to {output_file}")
