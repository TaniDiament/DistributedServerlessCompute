package edu.yu.cs.com3800.stage5;

public interface Client {
    class Response {
        private int code;
        private String body;
        private boolean cached;
        private String src;

        public Response(int code, String body, boolean cached, String src) {
            this.code = code;
            this.body = body;
            this.cached = cached;
            this.src = src;
        }

        public int getCode() {
            return this.code;
        }

        public String getBody() {
            return this.body;
        }

        public boolean getCached() {
            return this.cached;
        }

        public String getSrc() {
            return this.src;
        }
    }
}