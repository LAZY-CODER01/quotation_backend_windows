"""
Excel file processing utility for SnapQuote.

Converts Excel content to markdown format.
"""

import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)

def excel_to_markdown(excel_content: bytes) -> str:
    """
    Convert Excel content to markdown format.
    
    Args:
        excel_content (bytes): Excel file content as bytes
        
    Returns:
        str: Excel content converted to markdown format
    """
    try:
        # Create a BytesIO object from the Excel content
        excel_file = io.BytesIO(excel_content)
        
        # Read all sheets from the Excel file
        excel_data = pd.read_excel(excel_file, sheet_name=None)
        
        markdown_content = []
        markdown_content.append("# Excel Document Content\n\n")
        
        # Process each sheet
        for sheet_name, df in excel_data.items():
            markdown_content.append(f"## Sheet: {sheet_name}\n\n")
            
            if df.empty:
                markdown_content.append("*[Empty sheet]*\n\n")
                continue
            
            # Clean the dataframe
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.dropna(axis=1, how='all')  # Remove completely empty columns
            
            if df.empty:
                markdown_content.append("*[No data in sheet]*\n\n")
                continue
            
            # Convert to markdown table
            try:
                # Replace NaN values with empty strings for better display
                df_display = df.fillna('')
                
                # Convert dataframe to markdown table
                markdown_table = df_display.to_markdown(index=False, tablefmt='pipe')
                
                if markdown_table:
                    markdown_content.append(f"{markdown_table}\n\n")
                else:
                    markdown_content.append("*[Error converting sheet to table]*\n\n")
                    
            except Exception as e:
                logger.warning(f"Error converting sheet '{sheet_name}' to markdown: {str(e)}")
                # Fallback: show basic info about the sheet
                rows, cols = df.shape
                markdown_content.append(f"*[Sheet contains {rows} rows and {cols} columns but couldn't be converted to table]*\n\n")
                
                # Try to show column names at least
                if not df.columns.empty:
                    markdown_content.append("**Columns:** " + ", ".join(str(col) for col in df.columns) + "\n\n")
        
        if len(markdown_content) == 1:  # Only header, no content
            markdown_content.append("*[No readable content found in this Excel file]*\n")
        
        return "".join(markdown_content)
        
    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        return f"# Excel Processing Error\n\n*Error: {str(e)}*\n"