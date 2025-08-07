import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinancialReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        
        // Split the key prefix to identify type
        String keyStr = key.toString();

        double sum = 0.0;
        int count = 0;

        // For Cost Efficiency, we need two sums
        double totalRevenue = 0.0;
        double totalCost = 0.0;

        for (Text val : values) {
            String valStr = val.toString();

            try {
                // Handle each analysis type
                if (keyStr.startsWith("RevenueCountry:") || 
                    keyStr.startsWith("SalesTrendProfit:") || 
                    keyStr.startsWith("SalesTrendUnits:") || 
                    keyStr.startsWith("ItemPerformance:") || 
                    keyStr.startsWith("DemandForecast:")) {
                    
                    sum += Double.parseDouble(valStr);
                    count++;

                } else if (keyStr.startsWith("CostEfficiency:")) {
                    // Format: revenue,cost
                    String[] parts = valStr.split(",");
                    if (parts.length == 2) {
                        totalRevenue += Double.parseDouble(parts[0]);
                        totalCost += Double.parseDouble(parts[1]);
                    }
                }

            } catch (NumberFormatException e) {
                // Skip invalid numbers
                continue;
            }
        }

        // Output formatting
        if (keyStr.startsWith("CostEfficiency:")) {
            double margin = (totalRevenue != 0) ? ((totalRevenue - totalCost) / totalRevenue) * 100 : 0;
            context.write(key, new Text(String.format("Revenue: %.2f, Cost: %.2f, Margin: %.2f%%", totalRevenue, totalCost, margin)));
        } else {
            context.write(key, new Text(String.format("Total: %.2f", sum)));
        }
    }
}