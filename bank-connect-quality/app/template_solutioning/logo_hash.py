import fitz
import cv2
import os
from app.conf import *
from app.template_dashboard.utils import check_and_migrate
from app.pdf_utils import read_pdf

def dhash(image_path, hashSize=8):
    image = cv2.imread(image_path)
    if image is None:
        return 0
    image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    resized = cv2.resize(image, (hashSize + 1, hashSize))
    diff = resized[:, 1:] > resized[:, :-1]
    return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])

def get_images(statement_id, bank_name, password='',is_cc=False):
    # download this file from the bucket and save it somewhere
    key = f"{'cc_pdfs' if is_cc else 'pdf'}/{statement_id}_{bank_name}.pdf"
    tmp_file_path = f"/tmp/{statement_id}_{bank_name}.pdf"
    try:
        pdf_bucket_response = s3.get_object(
            Bucket = PDF_BUCKET, 
            Key=key
        )
        # write a temporary file with content
        with open(tmp_file_path, 'wb') as file_obj:
            file_obj.write(pdf_bucket_response['Body'].read())
    except Exception as e:
        print(e)
        check_and_migrate(statement_id, bank_name)
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        return {"message": "this file does not exist in the bucket"}

    doc = read_pdf(tmp_file_path, password)
    s3_links = {}
    
    if isinstance(doc, int):
        return s3_links
    
    for i in range(min(2, doc.page_count)):
        images = []
        
        try:
            images = doc.get_page_images(i)
        except RuntimeError:
            print("Unable to extract images")

            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            doc.close()
            return {}
        except Exception as e:
            print("Page Number is not available, maybe because it is a malformed pdf")
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            doc.close()
            return s3_links
        hashes_list = []
        
        images = images[:100]
        index = 0

        for img in images:
            xref = img[0]
            try:
                pix = fitz.Pixmap(doc, xref)
            except Exception as e:
                print("Exception because source colorspace is None")
                if os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)
                continue

            png_name = f"/tmp/{statement_id}_{bank_name}-{index}.png"
            index += 1
            try:
                if pix.n == 0:
                    pix.save(png_name)
                else:
                    pix1 = fitz.Pixmap(fitz.csRGB, pix)
                    pix1.save(png_name)
                # upload this png to s3 and save the link
                # the name of this s3 file is the image hash
                
                hash_image = dhash(png_name)
                s3_file_path = f"quality_logo/{hash_image}.png"

                s3_resource.Bucket(QUALITY_BUCKET).upload_file(
                    png_name, 
                    s3_file_path, 
                    ExtraArgs={
                        'Metadata': {
                                "statement_id": statement_id,
                                "bank_name": bank_name
                            }
                        }
                )

                s3_links[s3_file_path] = hash_image
                if os.path.exists(png_name):
                    os.remove(png_name)
            except Exception as e:
                print(e)
                print("Maybe a case of source colorspace None")
                # skip image for which couldn't write PNG file
                continue
    doc.close()
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)

    return s3_links