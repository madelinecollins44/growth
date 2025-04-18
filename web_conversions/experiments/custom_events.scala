def reviewEngagement(events: Vector[MiniEvent], startIndexes: Vector[Int]): Map[Int, Vector[Double]] = {
  val reviewEngagement = List(
    "listing_page_reviews_pagination", 
    "appreciation_photo_overlay_opened", 
    "listing_page_reviews_content_toggle_opened",
    "reviews_categorical_tag_clicked", 
    "sort_reviews",
    "shop_home_reviews_pagination",
    "inline_appreciation_photo_click_shop_page"
  )
  matchExistsOneOrZero(
    events,
    startIndexes,
    (e: MiniEvent) => reviewEngagement.contains(e.eventType)
  )
}
