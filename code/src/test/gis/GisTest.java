package gis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class GisTest {

    Gis gis = new Gis();

    public GisTest() throws IOException {
    }

    @Test
    public void assertRightAreaForCoordinates() throws IOException {
        gis.createPolygonFromGeoJson("24");
        Assert.assertSame(gis.getRegionOfPoint(48.84135874059337,  2.431329889833028), "24");
    }

    @Test
    public void assertWrongAreaForCoordinates() throws IOException {
        gis.createPolygonFromGeoJson("27");
        Assert.assertNotSame("24", gis.getRegionOfPoint(47.42944408233319,6.940990381448314));
    }
}