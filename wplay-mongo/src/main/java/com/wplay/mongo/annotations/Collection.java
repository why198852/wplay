package com.wplay.mongo.annotations;

import java.lang.annotation.*;

/** 
 * @author         lolog
 * @version        V1.0  
 * @Date           2016.07.10
 * @Company        WEGO
 * @Description    MongoDB
*/
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Collection {
	// ¼¯ºÏÃû
	public String name() default "";
}
