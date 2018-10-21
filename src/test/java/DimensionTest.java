import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.mr.service.IDimensionConvert;

import java.io.IOException;
import java.sql.SQLException;

public class DimensionTest {
    public static void main(String[] args) {
        IDimensionConvert convert = new IDimensionConvert() {
            @Override
            public int getDimensionIdByDimension(BaseDimension dimension) throws IOException, SQLException {
                return 0;
            }
        };
    }
}
