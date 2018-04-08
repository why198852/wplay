package com.wplay.mongo.annotations;

import java.lang.annotation.*;

/** 
 * @author         lolog
 * @version        V1.0  
 * @Date           2016.08.21
 * @Company        WEGO
 * @Description    update object 
*/

@Inherited 
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE,ElementType.FIELD})
public @interface EKey {
	// key value
	public String value() default "";
	// new's index
	public int index() default -1;
	// value priority, can't edit if true
	public boolean priority() default false;
	
}
