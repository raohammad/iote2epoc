package gis;

import com.esri.core.geometry.*;
import com.fasterxml.jackson.core.JsonParseException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by hammadakhan on 28/04/2019.
 */

public class Gis implements Serializable {

    HashMap<String, Rectangle> regionRectangles;

    public Gis() throws IOException {

        //GIS data initialization based on individual regions GeoJSON downloaded from
        //https://datanova.laposte.fr/explore/dataset/contours-geographiques-des-nouvelles-regions-metropole/table/

        String[] regions = {"11","24","27","28","32","44","52","53","75","76","84","93","94"};
        HashMap<String, Polygon> regionHash = new HashMap<>();
        regionRectangles = new HashMap<>();

        //initialize geometry polygons so that each point becomes a 2d point object in each polygon
        for(int i=0; i<regions.length; i++){
            regionHash.put(regions[i],createPolygonFromGeoJson(regions[i]));
        }

        //in order to define rectangular region of each polygon
        regionHash.forEach((k,v)->{
            //take all the points inside this polygon
            Point2D[] pts = v.getCoordinates2D();
            //initialize rectangle with first point
            Rectangle rectangle = new Rectangle(pts.length>0?pts[0]:new Point2D(0,0));

            //pass recntangle object through each 2D point
            for(int i=1; i<pts.length; i++){
                //System.out.println(pts[i].x+":"+ pts[i].y);
                rectangle.adjust(pts[i]);
                rectangle.toString();
            }
            regionRectangles.put(k,rectangle);
            //System.out.println("Final Rectangle for region:"+k);
            //System.out.println(regionRectangles.get(k).toString());

        });
    }

    public String getRegionOfPoint(double x, double y){
        AtomicReference<String> regionId = new AtomicReference<>("unspecified");
        //System.out.println("Number of Regions Identified:"+this.regionRectangles.size());
        this.regionRectangles.forEach((k,v)->{
            //System.out.println("X:"+x+"  Y:"+y+"     rectangleIs:"+v.toString());
            if(v.situatedInside(x,y)) {

                regionId.set(k);
            }
        });
        return regionId.get();
    }

    Polygon createPolygonFromGeoJson( String regionId) throws JsonParseException, IOException
    {
        //location of data files is hardcoded - can be cahanged relative
        String geoJsonContent = new String(Files.readAllBytes(Paths.get("/Users/lw5523/Downloads/data/singleGeoJson-"+regionId+".json")));
        MapGeometry mapGeom = OperatorImportFromGeoJson.local().execute(GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Polygon, geoJsonContent, null);

        return (Polygon)mapGeom.getGeometry();
    }
}
