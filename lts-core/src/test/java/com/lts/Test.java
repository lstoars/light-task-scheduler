package com.lts;

import com.lts.core.support.CronExpression;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by lenovo on 2016/3/30.
 */
public class Test {
	//Wed Mar 30 16:07:31 CST 2016
	public static void main(String[] args) throws ParseException {
		CronExpression expression = new CronExpression("* 0 0/1 * * ?");
		Date nextTime = expression.getTimeAfter(new Date());
		System.out.println(nextTime);
	}
}
