/**
 * Created by Nick on 12/5/16.
 */

import scala.Int;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkContext;

public class GetisCalculator {
    public static Double Hotspot(SparkContext sc, ArrayList<scala.Tuple2<String,scala.Int>> result,
                                 int cellnox, int cellnoy, int date,long sum, long squareSum,long count){



        List<Tuple2<String, Int>> neighbors = result;
        List<Integer> attrs = new ArrayList<Integer>();
        for(Object neighbor: neighbors){
            String tuple = neighbor.toString();
            tuple = tuple.replace("(", "");
            tuple = tuple.replace(")","");
            String[] vals = tuple.split(",");
            int temp = Integer.parseInt(vals[3]);
            attrs.add(temp);

        }
        Double z_score = GetisCalculator.getis_ord(cellnox,cellnoy,date,attrs,sum,squareSum,count);
        return z_score;
    }


    public static Double getis_ord(int cellnox, int cellnoy, int date, List<Integer> attrs,Long sum, Long squareSum,long count) {

        int numberOfNeighbours = attrs.size();
        int weight =1;
        int sumOfWeights = 0;
        int sumOfWeightsSquared =0;
        for (int i = 0; i < numberOfNeighbours; i++)
            sumOfWeights += weight;
        for (int i = 0; i < numberOfNeighbours; i++)
            sumOfWeightsSquared += weight*weight;

        Double sumOfValuesOfNeighbours = 0.0;
        Double mean = (double)sum /(double)count;
        Double sValue = Math.sqrt(((double)squareSum/ (double)count) - (mean*mean));

        for (Integer neighborVal: attrs){
            if (neighborVal != null)
                sumOfValuesOfNeighbours += neighborVal;
        }
        Double numerator = ((double) sumOfValuesOfNeighbours  -  (mean * sumOfWeights));
        Double denominatorSecondPart = Math.sqrt((double)((count * sumOfWeightsSquared) - (sumOfWeights * sumOfWeights)) /(double) (count - 1));
        Double denominator = sValue * denominatorSecondPart;
        Double getisScore = numerator / denominator;
        return getisScore;
    }
//    public static void main(String[] args){
//        System.out.println("hi");
//        //HotspotAnalysis obj = new HotspotAnalysis();
//
//        HotspotAnalysis$ h =  HotspotAnalysis$.MODULE$;
//        h.main(args);
//    }

}



