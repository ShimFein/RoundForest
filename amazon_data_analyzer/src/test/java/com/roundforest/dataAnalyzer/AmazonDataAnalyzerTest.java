package com.roundforest.dataAnalyzer;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;

public class AmazonDataAnalyzerTest {
	
	public static final String TEST_CSV_FILE_PATH = "src/test/resources";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Test
	public void testMostActiveUsers() {
		List<String> actualMostActiveUserList = null;
		List<String> expectedMostActiveUserList = new ArrayList<String>();

		AmazonDataAnalyzer testAnalyser = new AmazonDataAnalyzer();
		// Array sorted in alphabetical order
		expectedMostActiveUserList.add("Natalia Corres");
		expectedMostActiveUserList.add("Pamela G. Williams");
		expectedMostActiveUserList.add("Twoapennything");

		if (testAnalyser.readFile(TEST_CSV_FILE_PATH) == true) {
			List<Row> rowList = testAnalyser.findMostActiveUsers(3);
			actualMostActiveUserList = rowList.stream().map(row -> row.getString(0)).collect(Collectors.toList());
		}
		assertEquals(expectedMostActiveUserList, actualMostActiveUserList);
	}
	
	@Test
	public void testMostCommentedFoodItems() {
		List<String> actualMostCommentedFoodItemsList = null;
		List<String> expectedMostCommentedFoodItemsList = new ArrayList<String>();

		AmazonDataAnalyzer testAnalyser = new AmazonDataAnalyzer();
		// Array sorted in alphabetical order
		expectedMostCommentedFoodItemsList.add("B000E7L2R4");
		expectedMostCommentedFoodItemsList.add("B001E4KFG0");
		expectedMostCommentedFoodItemsList.add("B006K2ZZ7K");

		if (testAnalyser.readFile(TEST_CSV_FILE_PATH) == true) {
			List<Row> rowList = testAnalyser.findMostCommentedFoodItems(3);
			actualMostCommentedFoodItemsList = rowList.stream().map(row -> row.getString(0))
					.collect(Collectors.toList());
		}
		assertEquals(expectedMostCommentedFoodItemsList, actualMostCommentedFoodItemsList);
	}
	
	@Test
	public void testMostUsedWords() {
		List<String> actualMostUsedWordsList = null;
		List<String> expectedMostUsedWordsList = new ArrayList<String>();

		AmazonDataAnalyzer testAnalyser = new AmazonDataAnalyzer();
		// Array sorted in alphabetical order
		expectedMostUsedWordsList.add("a");
		expectedMostUsedWordsList.add("and");
		expectedMostUsedWordsList.add("the");

		if (testAnalyser.readFile(TEST_CSV_FILE_PATH) == true) {
			List<Row> rowList = testAnalyser.findMostUsedWords(3);
			actualMostUsedWordsList = rowList.stream().map(row -> row.getString(0))
					.collect(Collectors.toList());
		}
		assertEquals(expectedMostUsedWordsList, actualMostUsedWordsList);
	}

}
