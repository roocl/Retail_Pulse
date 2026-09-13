package com.retailpulse.offline;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RetailIngestorTest {
    @TempDir Path root;

    @Test
    void repeatedImportPreservesSourceRowsAndOriginalBytes() throws Exception {
        Path source = root.resolve("input.xlsx");
        try (var workbook = new XSSFWorkbook()) {
            var sheet = workbook.createSheet("Online Retail");
            String[] headers = {"InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"};
            var header = sheet.createRow(0);
            for (int i = 0; i < headers.length; i++) header.createCell(i).setCellValue(headers[i]);
            for (int r = 1; r <= 2; r++) {
                var row = sheet.createRow(r);
                String[] values = {"100", "A", "Gift", "2", "2011-01-01T12:00:00", "1.25", "", "UK"};
                for (int c = 0; c < values.length; c++) row.createCell(c).setCellValue(values[c]);
            }
            try (var output = Files.newOutputStream(source)) { workbook.write(output); }
        }
        var ingestor = new RetailIngestor(root.resolve("data"));
        Path batch = ingestor.ingest(source);
        assertEquals(batch, ingestor.ingest(source));
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(batch.resolve("source.xlsx")));
        var lines = Files.readAllLines(batch.resolve("rows.csv"));
        assertEquals(3, lines.size());
        assertNotEquals(lines.get(1).split(",")[0], lines.get(2).split(",")[0]);
        assertEquals(lines.get(1).split(",")[2], lines.get(2).split(",")[2]);
        assertTrue(lines.get(1).contains("2011-01-01T12:00:00"));
        Files.writeString(batch.resolve("rows.csv"), "corrupted");
        assertThrows(java.io.IOException.class, () -> ingestor.ingest(source));
    }

    @Test
    void invalidColumnsDoNotPublishAnInputBatch() throws Exception {
        Path source = root.resolve("invalid.xlsx");
        try (var workbook = new XSSFWorkbook()) {
            workbook.createSheet("Online Retail").createRow(0).createCell(0).setCellValue("Unexpected");
            try (var output = Files.newOutputStream(source)) { workbook.write(output); }
        }
        assertThrows(Exception.class, () -> new RetailIngestor(root.resolve("data")).ingest(source));
        try (var files = Files.walk(root.resolve("data"))) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().equals("manifest.json")));
        }
    }
}
