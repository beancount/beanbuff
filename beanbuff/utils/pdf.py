"""Utilities for converting PDF to text."""

import subprocess


def convert_pdf_to_text(filename: str) -> str:
    """Convert the contents of a filename to text, approximately."""
    pipe = subprocess.Popen(["pdftotext", "-v", filename, "-"],
                            shell=False,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    stderr_str = stderr.decode('utf-8')
    if pipe.returncode != 0:
        raise RuntimeError("Error {} in PDF conversion: {}".format(pipe.returncode, stderr))
    return stdout
