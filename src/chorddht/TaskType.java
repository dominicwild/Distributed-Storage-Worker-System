package chorddht;

/**
 * Defines a type of task. Where task is a type of processing.
 * @author Dominic
 */
public enum TaskType {
        WORD_METRICS("Word Metrics"), ENCRYPT("Encrypt"),HASH("Hash"), UNDEFINED("Undefined");

        public String type;        //String asscoiated with the task type.

        /**
         * Creates the TaskType with an associated string.
         * @param type The string for this TaskType.
         */
        TaskType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

        /**
         * Converts a given string to a TaskType.
         * @param type The string to convert to a type.
         * @return The TaskType that the string associates with.
         */
        public static TaskType stringToType(String type) {
            switch (type) {
                case "Word Metrics":
                    return WORD_METRICS;
                case "Encrypt":
                    return ENCRYPT;
                case "Hash":
                    return HASH;
                default:
                    return UNDEFINED;
            }
        }
    };
