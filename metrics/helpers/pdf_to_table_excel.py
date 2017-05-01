import os
import pandas as pd
from openpyxl import load_workbook


def pdf_to_table(hc, dbase, tables, pdfs):
    if not hasattr(tables, '__iter__'):
        tables = [tables]
        pdfs = [pdfs]
    for tab, pdf in zip(tables, pdfs):
        pdf2 = pdf.rename(columns=lambda c: c.replace(' ', '_'))
        df = hc.createDataFrame(pdf2)
        df.write.saveAsTable(dbase + '.' + tab, mode='overwrite')


def pdf_to_excel(output_path, tables, pdfs, dbase=''):
    if not hasattr(tables, '__iter__'):
        tables = [tables]
        pdfs = [pdfs]
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    fname = 'custom_metrics_' + dbase + '.xlsx'
    fp = output_path + '/' + fname
    exists = os.path.isfile(fp)
    writer = pd.ExcelWriter(fp, engine='openpyxl')
    if exists:
        book = load_workbook(fp)
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    for tab, pdf in zip(tables, pdfs):
        pdf.to_excel(writer, tab.replace('consulting_', ''), index=False)
    writer.save()
