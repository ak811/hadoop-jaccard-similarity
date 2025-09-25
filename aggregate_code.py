import os

# Define the folder path here
folder_path = ""  # "" means project root
output_file = "aggregated_code.txt"

# Exceptions
excluded_folders = ["__pycache__", "venv", ".git"]  # skip these folders
excluded_files = ["vit_qchunk_topk.yaml", "vit_qchunk.yaml"]  # skip these files

# If folder_path is empty, set it to the directory of this script (project root)
if not folder_path:
    folder_path = os.path.dirname(os.path.abspath(__file__))


def build_tree(start_path):
    """Return a string with the folder tree structure (like Linux `tree`)."""
    tree_lines = []
    for root, dirs, files in os.walk(start_path):
        # Remove excluded folders from traversal
        dirs[:] = [d for d in dirs if d not in excluded_folders]

        # Compute indentation level
        level = root.replace(start_path, "").count(os.sep)
        indent = " " * 4 * level
        tree_lines.append(f"{indent}{os.path.basename(root)}/")

        subindent = " " * 4 * (level + 1)
        for f in files:
            if f not in excluded_files:
                tree_lines.append(f"{subindent}{f}")
    return "\n".join(tree_lines)


with open(output_file, "w", encoding="utf-8") as out:
    # --- Write file tree first ---
    out.write("PROJECT FILE TREE\n")
    out.write("=" * 80 + "\n")
    out.write(build_tree(folder_path))
    out.write("\n\n\n")

    # --- Write file contents ---
    for root, dirs, files in os.walk(folder_path):
        dirs[:] = [d for d in dirs if d not in excluded_folders]

        for file in files:
            if file not in excluded_files:  # Exclude specific files
                file_path = os.path.join(root, file)

                # Write full path before content
                out.write(f"{file_path}\n")
                out.write("=" * 80 + "\n")

                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        out.write(f.read())
                except Exception as e:
                    out.write(f"Error reading file: {e}")

                out.write("\n\n")  # space between files

print(f"All files and file tree have been written to {output_file}")
