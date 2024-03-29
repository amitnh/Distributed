    import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.util.List;

    public class namedEntityRecognitionHandler {
        private static StanfordCoreNLP NERPipeline;
        public namedEntityRecognitionHandler() {
            Properties props = new Properties();
            props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
            NERPipeline = new StanfordCoreNLP(props);
        }
        public static String[] findEntities(String review) {
            String[] s = new String[]{"PERSON:","\nLOCATION:","\nORGANIZATION:"}; // PERSON, LOCATION, ORGANIZATION
// create an empty Annotation just with the given text
            Annotation document = new Annotation(review);
// run all Annotators on this text
            NERPipeline.annotate(document);
// these are all the sentences in this document
// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
            for (CoreMap sentence : sentences) {
// traversing the words in the current sentence
// a CoreLabel is a CoreMap with additional token-specific methods
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
// this is the text of the token
                    String word = token.get(CoreAnnotations.TextAnnotation.class);
// this is the NER label of the token
                    String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                    if (ne.equals("PERSON")) s[0] += word + ",";
                    else if (ne.equals("LOCATION")) s[1] += word + ",";
                    else if (ne.equals("ORGANIZATION")) s[2] += word + ",";
                }
            }
            //removing last comma
            if(s[0].length()>"PERSON:".length())
                s[0] =  s[0].substring(0,s[0].length()-1);

            if(s[1].length()>"\nLOCATION:".length())
                s[1] =  s[1].substring(0,s[1].length()-1);

            if(s[2].length()>"\nORGANIZATION:".length())
                s[2] =  s[2].substring(0,s[2].length()-1);
            return s;
        }
        public static void printEntities(String review) {
// create an empty Annotation just with the given text
            Annotation document = new Annotation(review);
// run all Annotators on this text
            NERPipeline.annotate(document);
// these are all the sentences in this document
// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
            for (CoreMap sentence : sentences) {
// traversing the words in the current sentence
// a CoreLabel is a CoreMap with additional token-specific methods
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
// this is the text of the token
                    String word = token.get(CoreAnnotations.TextAnnotation.class);
// this is the NER label of the token
                    String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                    System.out.println("\t-" + word + ":" + ne);
                }
            }
        }
    }

