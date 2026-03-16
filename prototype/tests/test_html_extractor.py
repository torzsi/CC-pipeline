from cc_pipeline.extractors import HTMLExtractor


def test_html_extractor_preserves_dom_order() -> None:
    html = """
    <html lang="en">
      <head><title>Example Title</title></head>
      <body>
        <article>
          <p>First paragraph.</p>
          <figure>
            <img src="/images/hero.jpg" width="800" height="600" alt="hero" />
            <figcaption>Figure caption.</figcaption>
          </figure>
          <p>Second paragraph.</p>
        </article>
      </body>
    </html>
    """

    result = HTMLExtractor().extract(html, page_url="https://example.com/post")

    assert result.title == "Example Title"
    assert result.language == "en"
    assert [type(slot).__name__ for slot in result.slots] == [
        "ExtractedText",
        "ExtractedImage",
        "ExtractedText",
        "ExtractedText",
    ]
    assert result.slots[1].source_url == "https://example.com/images/hero.jpg"
    assert result.slots[1].width == 800
    assert result.slots[1].height == 600


def test_html_extractor_uses_lazy_image_attributes() -> None:
    html = """
    <html>
      <body>
        <div>Intro text with enough content to survive filtering requirements.</div>
        <img data-src="https://cdn.example.com/image.png" />
      </body>
    </html>
    """

    result = HTMLExtractor().extract(html, page_url="https://example.com/post")

    assert result.slots[1].source_url == "https://cdn.example.com/image.png"
