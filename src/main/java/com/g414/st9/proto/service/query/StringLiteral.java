package com.g414.st9.proto.service.query;

/**
 * Helper class to encapsulate building a string literal with escapes in ANTLR.
 */
public class StringLiteral {
    private final StringBuilder buf = new StringBuilder();

    public void append(int achar) {
        buf.appendCodePoint(achar);
    }

    public void append(String chars) {
        buf.append(chars);
    }

    public void appendEscaped(String chars) {
        if (chars.startsWith("\\") && chars.length() == 2) {
            switch (chars.charAt(1)) {
            case 'n':
                buf.append("\n");
                break;
            case 't':
                buf.append("\t");
                break;
            case '\\':
                buf.append("\\");
                break;
            case '"':
                buf.append("\"");
                break;
            }
        } else {
            throw new IllegalArgumentException("Unexpected content: " + chars);
        }
    }

    @Override
    public String toString() {
        return buf.toString();
    }
}
