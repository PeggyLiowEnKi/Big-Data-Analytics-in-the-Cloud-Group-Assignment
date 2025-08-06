import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinancialMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            //Convert the current line of CSV to a string
            String line = value.toString();

            //Skip header 
            if (key.get() == 0 && line.contains("Region")) {
                return;
            }
            //Split into columns
            String[] fields = line.split(",");

            //Check if the row has all 14 expected fields
            if (fields.length != 14) {
                return; 
            }
            //Trim whitespace for each field
            for (int i = 0; i <fields.length; i++){
                fields[i] = fields[i].trim();
            }
            try {
                //Extract the 10 required fields
                String region = fields[0];
                String country = fields[1];
                String itemType = fields[2];
                String orderDate = fields[5];
                // Split order date (format mm/dd/yyyy)
                String[] dateParts = orderDate.split("/");
                if (dateParts.length != 3) {
                    return; 
                }
                String month = dateParts[0];
                String day = dateParts[1];
                String year = dateParts[2];
                String unitsSold = fields[8];
                String unitPrice = fields [9];
                String unitCost = fields [10];
                String totalRevenue = fields[11];
                String totalCost = fields[12];
                String totalProfit = fields[13];

                //Skip rows with empty important values
                if (region.isEmpty() || country.isEmpty() || itemType.isEmpty() || orderDate.isEmpty() || unitsSold.isEmpty() || unitPrice.isEmpty() || unitCost.isEmpty() || totalRevenue.isEmpty() || totalCost.isEmpty() || totalProfit.isEmpty()) {
                    return;
                }

                //Revenue Analysis by Country (total profit)
                context.write(new Text("RevenueCountry:" + country), new Text(totalProfit));

                //Sales Trend by Year- Total profit and units sold
                context.write(new Text("SalesTrendProfit:" + year), new Text(totalProfit));
                context.write(new Text("SalesTrendUnits:" + year), new Text(unitsSold));

                //High-Performing Product Category (Item Type by total sales)
                context.write(new Text("ItemPerformance:" + itemType), new Text(unitsSold));
                
                //Demand Forecasting by Item Type and Month
                context.write(new Text("DemandForecast:" + itemType + "," + year + "-" + month), new Text(unitsSold));

                //Cost efficiency analysis
                context.write(new Text("CostEfficiency:" + itemType), new Text(totalRevenue + "," + totalCost));

            } catch(Exception e){
                //log for debugging
                System.err.println("Error parsing line: " + e.getMessage());
                return;
            }

        } 
}

