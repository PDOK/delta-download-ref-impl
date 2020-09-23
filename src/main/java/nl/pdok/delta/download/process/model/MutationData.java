package nl.pdok.delta.download.process.model;

public class MutationData {
    public String id;
    public String feature;
    public String data;


    public static final class MutatieDataBuilder {
        public String id;
        public String feature;
        public String data;

        private MutatieDataBuilder() {
        }

        public static MutatieDataBuilder aMutatie() {
            return new MutatieDataBuilder();
        }

        public MutatieDataBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public MutatieDataBuilder withFeature(String feature) {
            this.feature = feature;
            return this;
        }

        public MutatieDataBuilder withData(String data) {
            this.data = data;
            return this;
        }

        public MutationData build() {
            MutationData mutationData = new MutationData();
            mutationData.id = this.id;
            mutationData.feature = this.feature;
            mutationData.data = this.data;
            return mutationData;
        }
    }
}
