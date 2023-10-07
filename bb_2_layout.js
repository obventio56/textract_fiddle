/**
 * How could this work?
 *
 * 1. Reduce to just elements with text property
 * 2. Bin everything into lines using Y coordinates
 * 3. Sort lines by X coordinates
 * 4. Convert to text. Diff < threshold, merge with space. Diff > threshold, merge with tab
 */

import optimalKMeansCluster from "./kmeans.js";
import axios from "axios";
import { writeFileSync } from "fs";
import * as math from "mathjs";

// It is stupid-difficult to do homography in JS so I put a server around python lol
const computeHomography = async (sourcePoints) => {
  const res = await axios.post("https://ef4a-18-117-160-75.ngrok-free.app", {
    points: sourcePoints.map((p) => [p.X, p.Y]),
  });

  return res.data;
};

function transformPoint(point, H) {
  const vec = math.transpose([point.X, point.Y, 1]);
  const result = math.multiply(H, vec);

  return {
    X: result[0] / result[2],
    Y: result[1] / result[2],
  };
}

const bb2Layout = async (awsResponse, azureResponse) => {
  const pages = awsResponse
    .filter((block) => block.BlockType === "PAGE")
    .reduce((cum, page) => {
      return [...cum, { ...page, lines: [] }];
    }, []);

  for (const page of azureResponse) {
    pages.find((p) => p.Page === page.page).lines = page.lines;
  }

  // Sort pages by Page
  pages.sort((a, b) => a.Page - b.Page);

  const pageTexts = await Promise.all(pages.map(processPage));
  const text = pageTexts.reduce((cum, pageText, idx) => {
    return cum + `\n ------ BEGIN PAGE ${idx + 1} ------ \n` + pageText;
  }, "");

  return text;
};

/**
 * Textract gives us polygon points clockwise starting from top left.
 * This is relative to the "top" of the document based on the orientation of the text.
 * Since this is true we can map the points to a standard rectangle.
 */
const processPage = async (page) => {
  // TODO: What if there's more than one page
  const homography = await computeHomography(page.Geometry.Polygon);

  const lines = page.lines;

  for (const line of lines) {
    const transformedPolygon = line.Polygon.map((p) =>
      transformPoint(p, homography)
    );

    line.TransformedBoundingBox = {
      Top: (transformedPolygon[0].Y + transformedPolygon[1].Y) / 2,
      Height: Math.abs(transformedPolygon[0].Y - transformedPolygon[3].Y),
      Left: (transformedPolygon[0].X + transformedPolygon[3].X) / 2,
      Width: Math.abs(transformedPolygon[0].X - transformedPolygon[1].X),
    };
  }

  const heights = lines.map((element) => element.TransformedBoundingBox.Height);
  const binnedHeights = optimalKMeansCluster(heights);

  // Find the largest member of the bin with the most members
  const largestBin = binnedHeights.reduce(
    (largestBin, bin) => (bin.length > largestBin.length ? bin : largestBin),
    []
  );

  // Divide lines that are greater than
  const lineHeight = Math.max(...largestBin) / 3;

  // Sort lines by top, ascending
  lines.sort(
    (a, b) => a.TransformedBoundingBox.Top - b.TransformedBoundingBox.Top
  );

  // Bin elements into lines
  const lineBins = [];
  let currentLine = [];
  let currentLineY = lines[0].TransformedBoundingBox.Top;
  for (let i = 0; i < lines.length; i++) {
    const element = lines[i];
    const elementY = element.TransformedBoundingBox.Top;

    if (Math.abs(elementY - currentLineY) > lineHeight) {
      lineBins.push(currentLine);
      currentLine = [];
    }
    currentLineY = elementY;

    currentLine.push(element);
  }

  if (currentLine.length > 0) {
    lineBins.push(currentLine);
  }

  // Sort each line by x, ascending. Then convert to text.
  const linesWithText = lineBins.map((line) => {
    const sortedLine = line.sort(
      (a, b) => a.TransformedBoundingBox.Left - b.TransformedBoundingBox.Left
    );

    const totalWidth = sortedLine.reduce(
      (totalWidth, element) =>
        totalWidth + element.TransformedBoundingBox.Width,
      0
    );
    const totalChars = sortedLine.reduce(
      (totalChars, element) => totalChars + element.text.length,
      0
    );

    const averageCharWidth = totalWidth / totalChars;

    // Anything with gap larger than averageCharWidth is a tab. Anything smaller is a space.
    const text = sortedLine.reduce((text, element, index) => {
      const nextElement = sortedLine[index + 1];
      const gap = nextElement
        ? nextElement.TransformedBoundingBox.Left -
          element.TransformedBoundingBox.Left -
          element.TransformedBoundingBox.Width
        : 0;

      return text + element.text + (gap > averageCharWidth * 2 ? "\t" : " ");
    }, "");
    return text;
  });

  // join lines with newline
  const text = linesWithText.join("\n");
  return text;
};

export { bb2Layout };
