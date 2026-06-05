package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"djangormtest/models/djangormtestapp"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Read database connection parameters from environment variables
	dbHost := os.Getenv("DATABASE_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbPortStr := os.Getenv("DATABASE_PORT")
	if dbPortStr == "" {
		dbPortStr = "5432"
	}
	dbPort, err := strconv.Atoi(dbPortStr)
	if err != nil {
		log.Fatalf("Invalid DATABASE_PORT: %v", err)
	}

	dbName := os.Getenv("DATABASE_NAME")
	if dbName == "" {
		dbName = "postgres"
	}

	dbUser := os.Getenv("DATABASE_USER")
	if dbUser == "" {
		dbUser = "postgres"
	}

	dbPassword := os.Getenv("DATABASE_PASSWORD")
	if dbPassword == "" {
		dbPassword = "postgres"
	}

	// Construct connection string
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		dbUser, dbPassword, dbHost, dbPort, dbName,
	)

	// Create connection pool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Test database connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Run test cases
	if err := testBasicQueries(ctx, pool); err != nil {
		log.Fatalf("testBasicQueries failed: %v", err)
	}

	if err := testFilteringByMultipleFields(ctx, pool); err != nil {
		log.Fatalf("testFilteringByMultipleFields failed: %v", err)
	}

	if err := testNullableFieldFiltering(ctx, pool); err != nil {
		log.Fatalf("testNullableFieldFiltering failed: %v", err)
	}

	if err := testNullableFieldsInResults(ctx, pool); err != nil {
		log.Fatalf("testNullableFieldsInResults failed: %v", err)
	}

	if err := testForeignKeyRelationships(ctx, pool); err != nil {
		log.Fatalf("testForeignKeyRelationships failed: %v", err)
	}

	if err := testReviewsWithNullValues(ctx, pool); err != nil {
		log.Fatalf("testReviewsWithNullValues failed: %v", err)
	}

	if err := testOrdering(ctx, pool); err != nil {
		log.Fatalf("testOrdering failed: %v", err)
	}

	log.Println("✓ All tests passed!")
}

func compareIP(a net.IP, b net.IP) int {
	aa := a.To16()
	bb := b.To16()
	if aa == nil || bb == nil {
		return 0
	}
	return bytes.Compare(aa, bb)
}

// testBasicQueries verifies basic SELECT queries with simple filtering
func testBasicQueries(ctx context.Context, db *pgxpool.Pool) error {
	// Query all authors - expect 4
	allAuthors, err := djangormtestapp.AuthorQS{}.All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get all authors: %w", err)
	}
	if len(allAuthors) != 4 {
		return fmt.Errorf("expected 4 authors, got %d", len(allAuthors))
	}

	// Query specific author by name - expect John Doe with ID 1
	authors, err := djangormtestapp.AuthorQS{}.NameEq("John Doe").All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query author by name: %w", err)
	}
	if len(authors) != 1 {
		return fmt.Errorf("expected 1 author named 'John Doe', got %d", len(authors))
	}
	if authors[0].GetID() != 1 {
		return fmt.Errorf("John Doe should have ID 1, got %d", authors[0].GetID())
	}

	// Query all articles - expect 8
	allArticles, err := djangormtestapp.ArticleQS{}.All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get all articles: %w", err)
	}
	if len(allArticles) != 8 {
		return fmt.Errorf("expected 8 articles, got %d", len(allArticles))
	}

	// Query first article - expect ID 1 with title "Django Basics"
	firstArticle, err := djangormtestapp.ArticleQS{}.OrderByID().First(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get first article: %w", err)
	}
	if firstArticle.GetID() != 1 {
		return fmt.Errorf("first article should have ID 1, got %d", firstArticle.GetID())
	}
	if firstArticle.Title != "Django Basics" {
		return fmt.Errorf("first article should be 'Django Basics', got '%s'", firstArticle.Title)
	}

	return nil
}

// testFilteringByMultipleFields verifies combining multiple filter conditions
func testFilteringByMultipleFields(ctx context.Context, db *pgxpool.Pool) error {
	// Find published articles with high ratings (>= 4.5) - expect 5
	highRatedArticles, err := djangormtestapp.ArticleQS{}.
		IsPublishedEq(true).
		RatingGe(4.5).
		OrderByRatingDesc().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query published articles with high ratings: %w", err)
	}
	if len(highRatedArticles) != 5 {
		return fmt.Errorf("expected 5 published articles with rating >= 4.5, got %d", len(highRatedArticles))
	}
	// Verify ordering is descending (first should have highest rating)
	if highRatedArticles[0].Rating == nil || *highRatedArticles[0].Rating < 4.9 {
		return fmt.Errorf("first high-rated article should have rating 4.9, got %v", highRatedArticles[0].Rating)
	}

	// Find draft articles (not published) - expect 2
	draftArticles, err := djangormtestapp.ArticleQS{}.
		IsPublishedEq(false).
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query draft articles: %w", err)
	}
	if len(draftArticles) != 2 {
		return fmt.Errorf("expected 2 draft articles, got %d", len(draftArticles))
	}
	// All should be unpublished
	for _, article := range draftArticles {
		if article.IsPublished != nil && *article.IsPublished {
			return fmt.Errorf("draft article '%s' should not be published", article.Title)
		}
	}

	// Find articles with specific priority (1) - expect 3
	priorityArticles, err := djangormtestapp.ArticleQS{}.
		PriorityEq(1).
		OrderByViewCountDesc().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query priority articles: %w", err)
	}
	if len(priorityArticles) != 3 {
		return fmt.Errorf("expected 3 articles with priority=1, got %d", len(priorityArticles))
	}

	return nil
}

// testNullableFieldFiltering verifies IS NULL and IS NOT NULL filtering
func testNullableFieldFiltering(ctx context.Context, db *pgxpool.Pool) error {
	// Find articles with publish time set - expect 6
	articlesWithTime, err := djangormtestapp.ArticleQS{}.
		PublishTimeIsNotNull().
		OrderByID().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query articles with publish time: %w", err)
	}
	if len(articlesWithTime) != 6 {
		return fmt.Errorf("expected 6 articles with publish_time IS NOT NULL, got %d", len(articlesWithTime))
	}
	// All should be non-null
	for _, article := range articlesWithTime {
		if article.PublishTime == nil {
			return fmt.Errorf("article '%s' should have publish_time set", article.Title)
		}
	}

	// Find articles without publish time - expect 2
	articlesWithoutTime, err := djangormtestapp.ArticleQS{}.
		PublishTimeIsNull().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query articles without publish time: %w", err)
	}
	if len(articlesWithoutTime) != 2 {
		return fmt.Errorf("expected 2 articles with publish_time IS NULL, got %d", len(articlesWithoutTime))
	}
	// All should be null
	for _, article := range articlesWithoutTime {
		if article.PublishTime != nil {
			return fmt.Errorf("article '%s' should not have publish_time set", article.Title)
		}
	}

	// Find authors with bio - expect 3
	authorsWithBio, err := djangormtestapp.AuthorQS{}.
		BioIsNotNull().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query authors with bio: %w", err)
	}
	if len(authorsWithBio) != 3 {
		return fmt.Errorf("expected 3 authors with bio IS NOT NULL, got %d", len(authorsWithBio))
	}

	// Find authors without bio - expect 1
	authorsWithoutBio, err := djangormtestapp.AuthorQS{}.
		BioIsNull().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query authors without bio: %w", err)
	}
	if len(authorsWithoutBio) != 1 {
		return fmt.Errorf("expected 1 author with bio IS NULL, got %d", len(authorsWithoutBio))
	}

	return nil
}

// testNullableFieldsInResults verifies checking null values in returned objects
func testNullableFieldsInResults(ctx context.Context, db *pgxpool.Pool) error {
	articles, err := djangormtestapp.ArticleQS{}.All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get articles: %w", err)
	}

	// Find article with ID 7 (CI/CD Pipelines) - should have null publish_time and author_mac
	var article7 *djangormtestapp.Article
	for _, a := range articles {
		if a.GetID() == 7 {
			article7 = a
			break
		}
	}
	if article7 == nil {
		return fmt.Errorf("article with ID 7 not found")
	}
	if article7.PublishTime != nil {
		return fmt.Errorf("article 7 (CI/CD Pipelines) should have NULL publish_time, got %v", article7.PublishTime)
	}
	if article7.AuthorMac != nil {
		return fmt.Errorf("article 7 (CI/CD Pipelines) should have NULL author_mac, got %v", article7.AuthorMac)
	}

	// Find article with ID 4 (API Design) - should have null publish_time and large_number
	var article4 *djangormtestapp.Article
	for _, a := range articles {
		if a.GetID() == 4 {
			article4 = a
			break
		}
	}
	if article4 == nil {
		return fmt.Errorf("article with ID 4 not found")
	}
	if article4.PublishTime != nil {
		return fmt.Errorf("article 4 (API Design) should have NULL publish_time, got %v", article4.PublishTime)
	}
	if article4.LargeNumber != nil {
		return fmt.Errorf("article 4 (API Design) should have NULL large_number, got %v", article4.LargeNumber)
	}

	// Verify all articles have non-null content (based on fixture)
	for _, article := range articles {
		if article.Content == nil {
			return fmt.Errorf("article '%s' should have content (not null in fixture)", article.Title)
		}
	}

	return nil
}

// testForeignKeyRelationships verifies querying related objects
func testForeignKeyRelationships(ctx context.Context, db *pgxpool.Pool) error {
	// Get article 1 and verify its author is John Doe (ID 1)
	article1Articles, err := djangormtestapp.ArticleQS{}.
		IDEq(1).
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get article 1: %w", err)
	}
	if len(article1Articles) != 1 {
		return fmt.Errorf("expected 1 article with ID 1, got %d", len(article1Articles))
	}

	article1 := article1Articles[0]
	author, err := article1.GetAuthor(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get author for article 1: %w", err)
	}
	if author == nil {
		return fmt.Errorf("article 1 author should not be nil")
	}
	if author.GetID() != 1 {
		return fmt.Errorf("article 1 author should be ID 1, got %d", author.GetID())
	}
	if author.Name != "John Doe" {
		return fmt.Errorf("article 1 author should be John Doe, got %s", author.Name)
	}

	// Query articles by author 1 (John Doe) - expect 3 articles
	johnArticles, err := djangormtestapp.ArticleQS{}.AuthorRawEq(1).All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query articles by author 1: %w", err)
	}
	if len(johnArticles) != 3 {
		return fmt.Errorf("expected 3 articles by author 1 (John Doe), got %d", len(johnArticles))
	}

	// Verify all those articles belong to author 1
	for _, article := range johnArticles {
		if article.GetAuthorRaw() != 1 {
			return fmt.Errorf("article '%s' should belong to author 1, got %d", article.Title, article.GetAuthorRaw())
		}
	}

	return nil
}

// testReviewsWithNullValues verifies complex queries with array fields and null values
func testReviewsWithNullValues(ctx context.Context, db *pgxpool.Pool) error {
	// Find reviews with rating set - expect 8 (all have ratings in fixture)
	reviewsWithRating, err := djangormtestapp.ReviewQS{}.
		RatingIsNotNull().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query reviews with rating: %w", err)
	}
	if len(reviewsWithRating) != 8 {
		return fmt.Errorf("expected 8 reviews with rating IS NOT NULL, got %d", len(reviewsWithRating))
	}

	// Find reviews without IP address - expect 1 (Diana - review ID 4)
	reviewsWithoutIP, err := djangormtestapp.ReviewQS{}.
		ReviewerIpIsNull().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query reviews without IP: %w", err)
	}
	if len(reviewsWithoutIP) != 1 {
		return fmt.Errorf("expected 1 review with reviewer_ip IS NULL, got %d", len(reviewsWithoutIP))
	}
	if reviewsWithoutIP[0].ReviewerIp != nil {
		return fmt.Errorf("review should have NULL reviewer_ip")
	}

	// Find reviews with rating 5 - expect 5 reviews
	highRatingReviews, err := djangormtestapp.ReviewQS{}.
		RatingEq(5).
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query high rating reviews: %w", err)
	}
	if len(highRatingReviews) != 5 {
		return fmt.Errorf("expected 5 reviews with rating=5, got %d", len(highRatingReviews))
	}
	// All should have rating 5
	for _, review := range highRatingReviews {
		if review.Rating == nil || *review.Rating != 5 {
			return fmt.Errorf("review should have rating 5, got %v", review.Rating)
		}
	}

	// Find reviews for article 1 - expect 2 (Alice and Bob)
	article1Reviews, err := djangormtestapp.ReviewQS{}.
		ArticleRawEq(1).
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query reviews for article 1: %w", err)
	}
	if len(article1Reviews) != 2 {
		return fmt.Errorf("expected 2 reviews for article 1, got %d", len(article1Reviews))
	}

	// Verify tags array is not null for these reviews
	for _, review := range article1Reviews {
		if review.Tags == nil {
			return fmt.Errorf("review '%s' should have tags array (not null)", review.ReviewerName)
		}
	}

	return nil
}

// testOrdering verifies query results ordering is correct
func testOrdering(ctx context.Context, db *pgxpool.Pool) error {
	// Order articles by view count (ascending) - verify ascending order
	articlesByViews, err := djangormtestapp.ArticleQS{}.
		OrderByViewCount().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to order articles by view count: %w", err)
	}
	if len(articlesByViews) != 8 {
		return fmt.Errorf("expected 8 articles, got %d", len(articlesByViews))
	}

	// Verify ascending order
	for i := 1; i < len(articlesByViews); i++ {
		prev := *articlesByViews[i-1].ViewCount
		curr := *articlesByViews[i].ViewCount
		if prev > curr {
			return fmt.Errorf("articles not in ascending order by view_count: %d > %d", prev, curr)
		}
	}
	// First should be 350 (API Design)
	if *articlesByViews[0].ViewCount != 350 {
		return fmt.Errorf("first article should have 350 views, got %d", *articlesByViews[0].ViewCount)
	}
	// Last should be 3100 (Database Optimization)
	if *articlesByViews[len(articlesByViews)-1].ViewCount != 3100 {
		return fmt.Errorf("last article should have 3100 views, got %d", *articlesByViews[len(articlesByViews)-1].ViewCount)
	}

	// Order articles by rating (descending) - verify descending order
	articlesByRating, err := djangormtestapp.ArticleQS{}.
		OrderByRatingDesc().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to order articles by rating: %w", err)
	}

	// Verify descending order
	for i := 1; i < len(articlesByRating); i++ {
		prev := *articlesByRating[i-1].Rating
		curr := *articlesByRating[i].Rating
		if prev < curr {
			return fmt.Errorf("articles not in descending order by rating: %.1f < %.1f", prev, curr)
		}
	}
	// First should be 4.9 (Security Best Practices)
	if *articlesByRating[0].Rating != 4.9 {
		return fmt.Errorf("first article should have rating 4.9, got %.1f", *articlesByRating[0].Rating)
	}

	// Order authors by name (ascending)
	authorsByName, err := djangormtestapp.AuthorQS{}.
		OrderByName().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to order authors by name: %w", err)
	}
	if len(authorsByName) != 4 {
		return fmt.Errorf("expected 4 authors, got %d", len(authorsByName))
	}

	// Verify ascending alphabetical order
	expectedNames := []string{"Alice Williams", "Bob Johnson", "Jane Smith", "John Doe"}
	for i, author := range authorsByName {
		if author.Name != expectedNames[i] {
			return fmt.Errorf("author %d should be '%s', got '%s'", i, expectedNames[i], author.Name)
		}
	}

	// Order articles by AuthorIp (ascending) - verify INET/numeric ordering
	articlesByIP, err := djangormtestapp.ArticleQS{}.
		AuthorIpIsNotNull().
		OrderByAuthorIp().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to order articles by author_ip: %w", err)
	}
	if len(articlesByIP) != 7 {
		return fmt.Errorf("expected 7 non-null author_ip articles, got %d", len(articlesByIP))
	}

	// Numeric expected order for fixture IPs: 2.0.0.2, 2.0.0.15, 10.0.0.2, 10.0.0.3, 10.0.0.10, 172.16.0.1, 203.0.113.1
	expectedIPOrder := []int64{1, 8, 2, 4, 3, 5, 6}
	for i, article := range articlesByIP {
		if article.GetID() != expectedIPOrder[i] {
			return fmt.Errorf("unexpected author_ip order at index %d: expected article ID %d, got %d", i, expectedIPOrder[i], article.GetID())
		}
	}

	// Verify numeric ascending order by IP bytes
	for i := 1; i < len(articlesByIP); i++ {
		prev := articlesByIP[i-1].AuthorIp
		curr := articlesByIP[i].AuthorIp
		if prev == nil || curr == nil {
			return fmt.Errorf("author_ip should not be nil after AuthorIpIsNotNull filter")
		}
		if compareIP(*prev, *curr) > 0 {
			return fmt.Errorf("articles not in ascending numeric order by author_ip: %s > %s", prev.String(), curr.String())
		}
	}

	// Guard against accidental lexical-string assertions.
	// This fixture intentionally makes lexical string order differ from numeric INET order.
	hasLexicalInversion := false
	for i := 1; i < len(articlesByIP); i++ {
		if articlesByIP[i-1].AuthorIp.String() > articlesByIP[i].AuthorIp.String() {
			hasLexicalInversion = true
			break
		}
	}
	if !hasLexicalInversion {
		return fmt.Errorf("fixture no longer demonstrates lexical-vs-INET ordering difference for author_ip")
	}

	// Order articles by AuthorInet (ascending) - verify INET ordering (IPNet)
	articlesByInet, err := djangormtestapp.ArticleQS{}.
		AuthorInetIsNotNull().
		OrderByAuthorInet().
		All(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to order articles by author_inet: %w", err)
	}
	if len(articlesByInet) != 7 {
		return fmt.Errorf("expected 7 non-null author_inet articles, got %d", len(articlesByInet))
	}

	// Expected order mirrors author_ip for /32 INET entries.
	expectedInetOrder := []int64{1, 8, 2, 4, 3, 5, 6}
	for i, article := range articlesByInet {
		if article.GetID() != expectedInetOrder[i] {
			return fmt.Errorf("unexpected author_inet order at index %d: expected article ID %d, got %d", i, expectedInetOrder[i], article.GetID())
		}
	}

	// Verify numeric ascending order by network IP bytes
	for i := 1; i < len(articlesByInet); i++ {
		prev := articlesByInet[i-1].AuthorInet
		curr := articlesByInet[i].AuthorInet
		if prev == nil || curr == nil {
			return fmt.Errorf("author_inet should not be nil after AuthorInetIsNotNull filter")
		}
		if compareIP(prev.IP, curr.IP) > 0 {
			return fmt.Errorf("articles not in ascending numeric order by author_inet: %s > %s", prev.String(), curr.String())
		}
	}

	hasInetLexicalInversion := false
	for i := 1; i < len(articlesByInet); i++ {
		if articlesByInet[i-1].AuthorInet.String() > articlesByInet[i].AuthorInet.String() {
			hasInetLexicalInversion = true
			break
		}
	}
	if !hasInetLexicalInversion {
		return fmt.Errorf("fixture no longer demonstrates lexical-vs-INET ordering difference for author_inet")
	}

	return nil
}
