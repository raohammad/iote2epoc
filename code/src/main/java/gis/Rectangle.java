package gis;

import com.esri.core.geometry.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * Created by hammadakhan on 28/04/2019.
 */

@AllArgsConstructor
@ToString
public class Rectangle implements Serializable {

    Point2D topleft;
    Point2D bottomleft;
    Point2D topright;
    Point2D bottomright;

    public Rectangle(Point2D init){
        this.topleft = new Point2D(init.x,init.y);
        this.topleft = new Point2D(init.x,init.y);
        this.topright = new Point2D(init.x,init.y);
        this.topright = new Point2D(init.x,init.y);
        this.bottomleft = new Point2D(init.x,init.y);
        this.bottomleft = new Point2D(init.x,init.y);
        this.bottomright = new Point2D(init.x,init.y);
        this.bottomright = new Point2D(init.x,init.y);
    }

    public void adjust(Point2D newPoint){

        if(newPoint.x < this.bottomleft.x || newPoint.x < this.topleft.x){
            this.topleft.x = newPoint.x;
            this.bottomleft.x = newPoint.x;
        }

        if(newPoint.x > this.topright.x || newPoint.x > this.bottomright.x){
            this.topright.x = newPoint.x;
            this.bottomright.x = newPoint.x;
        }

        if(newPoint.y > this.topright.y || newPoint.y > this.topleft.y){
            this.topright.y = newPoint.y;
            this.topleft.y = newPoint.y;
        }

        if(newPoint.y < this.bottomright.y || newPoint.y > this.bottomleft.y){
            this.bottomright.y = newPoint.y;
            this.bottomleft.y = newPoint.y;
        }

    }

    public Point2D getTopleft() {
        return topleft;
    }

    public void setTopleft(Point2D topleft) {
        this.topleft = topleft;
    }

    public Point2D getBottomleft() {
        return bottomleft;
    }

    public void setBottomleft(Point2D bottomleft) {
        this.bottomleft = bottomleft;
    }

    public Point2D getTopright() {
        return topright;
    }

    public void setTopright(Point2D topright) {
        this.topright = topright;
    }

    public Point2D getBottomright() {
        return bottomright;
    }

    public void setBottomright(Point2D bottomright) {
        this.bottomright = bottomright;
    }

    public boolean situatedInside(double x, double y) {
        //X and Y are switched somehow :)) took my 30minutes when no point was overlapping.
        // Finally just had to switch the values-will see later whats causing this either input or my own transformation from json to Jobject
        if(y>this.bottomleft.x && y<this.topright.x && x <this.topright.y && x > this.bottomleft.y)
            return true;
        else
            return false;
    }
}
