import os
from pathlib import Path
import pypandoc

def convert_markdown_to_pdf(input_dir):
    # Obtener el directorio actual
    current_dir = Path(input_dir)
    
    # Buscar todos los archivos markdown (con extensión .markdown) en el directorio y subdirectorios
    md_files = list(current_dir.rglob('*.markdown'))
    
    if not md_files:
        print("No se encontraron archivos markdown en el directorio especificado.")
        return
    
    # Descargar pandoc si no está instalado
    try:
        pypandoc.get_pandoc_version()
    except OSError:
        print("Descargando pandoc...")
        pypandoc.download_pandoc()
    
    # Convertir cada archivo markdown a PDF
    for md_file in md_files:
        try:
            # Crear el subdirectorio 'pdf' dentro del directorio del archivo markdown
            pdf_dir = md_file.parent / 'pdf'
            pdf_dir.mkdir(exist_ok=True)
            
            # Construir el nombre del archivo PDF
            pdf_file = pdf_dir / f"{md_file.stem}.pdf"
            
            # Convertir usando pandoc
            print(f"Convertiendo {md_file.name}...")
            output = pypandoc.convert_file(
                str(md_file),
                'pdf',
                format='markdown',
                outputfile=str(pdf_file),
                extra_args=['--pdf-engine=wkhtmltopdf']
            )
            
            if output is None:
                print(f"✅ Convertido {md_file.name} a {pdf_file.name}")
            else:
                print(f"❌ Error convirtiendo {md_file.name}")
                
        except Exception as e:
            print(f"❌ Error convirtiendo {md_file.name}: {str(e)}")
            output = pypandoc.convert_file(
                str(md_file),
                'pdf',
                format='markdown',
                outputfile=str(pdf_file),
                extra_args=['--pdf-engine=xelatex']
            )
            
            if output is None:
                print(f"Convertido {md_file.name} a {pdf_file.name}")
            else:
                print(f"Error convirtiendo {md_file.name}")
                
        except Exception as e:
            print(f"Error convirtiendo {md_file.name}: {str(e)}")

if __name__ == "__main__":
    # Obtener el directorio del proyecto
    project_dir = Path(__file__).parent.parent
    
    # Directorio donde están los archivos markdown
    input_dir = str(project_dir / "data" / "images" / "formatted_tables_v2")
    
    print(f"Convirtiendo archivos markdown de {input_dir} a PDF...")
    convert_markdown_to_pdf(input_dir)
    print("\nProceso de conversión completado.")
