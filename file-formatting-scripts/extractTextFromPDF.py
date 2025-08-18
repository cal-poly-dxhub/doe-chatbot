import os
import fitz  # PyMuPDF

def extract_text_from_pdf(pdf_path):
    """Extracts text from a PDF file."""
    doc = fitz.open(pdf_path)
    text = "\n".join([page.get_text("text") for page in doc])
    doc.close()
    return text

def process_pdfs(directory):
    """Extracts text from all PDFs in a directory and saves to .txt files."""
    for file in os.listdir(directory):
        if file.lower().endswith(".pdf"):
            pdf_path = os.path.join(directory, file)
            text = extract_text_from_pdf(pdf_path)
            txt_path = os.path.join(directory, os.path.splitext(file)[0] + ".txt")

            with open(txt_path, "w", encoding="utf-8") as txt_file:
                txt_file.write(text)
            
            print(f"Extracted text from: {file} -> {txt_path}")

if __name__ == "__main__":
    process_pdfs("/home/ubuntu/policyDocsIngest/pdfs")  # Runs in the current directory
