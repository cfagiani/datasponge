package org.cataractsoftware.datasponge.util;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Wrapper over pdf manipulation libraries
 *  @author Christopher Fagiani
 */
public class PdfUtil {

    /**
     * extracts the textual content of a pdf loaded from the URL passed in as a single string
     * @param url
     * @return
     * @throws IOException
     */
    public static String extractTextFromPdf(String url) throws IOException {
        return extractText(PDDocument.load(new URL(url)));
    }

    /**
     * extracts the textual content of a pdf loaded from the URL passed in as a single string
     * @param file
     * @return
     * @throws IOException
     */
    public static String extractTextFromPdf(File file) throws IOException {
        return extractText(PDDocument.load(file));
    }

    private static String extractText(PDDocument pdfDoc) throws IOException{
        PDFTextStripper textStripper = new PDFTextStripper();
        return textStripper.getText(pdfDoc);
    }
}
