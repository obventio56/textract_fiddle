function kmeans(data, k) {
  // 1. Randomly select centroids
  let centroids = initializeCentroids(data, k);
  let prevCentroids = [];
  let clusters = [];

  while (!isEqual(centroids, prevCentroids)) {
    clusters = formClusters(data, centroids);

    prevCentroids = centroids;
    centroids = recalculateCentroids(clusters);
  }

  return clusters;
}

function initializeCentroids(data, k) {
  let centroids = [];
  centroids.push(data[Math.floor(Math.random() * data.length)]);

  for (let i = 1; i < k; i++) {
    let distances = data.map((point) => {
      return Math.min(
        ...centroids.map((centroid) => Math.abs(centroid - point) ** 2)
      );
    });

    let totalDistance = distances.reduce((a, b) => a + b, 0);
    let randValue = Math.random() * totalDistance;

    for (let j = 0, sum = 0; j < distances.length; j++) {
      sum += distances[j];
      if (randValue <= sum) {
        centroids.push(data[j]);
        break;
      }
    }
  }

  return centroids;
}

function formClusters(data, centroids) {
  let clusters = Array.from({ length: centroids.length }, () => []);

  data.forEach((point) => {
    let closestCentroidIndex = 0;
    let minDistance = Infinity;

    centroids.forEach((centroid, index) => {
      let distance = Math.abs(centroid - point);
      if (distance < minDistance) {
        closestCentroidIndex = index;
        minDistance = distance;
      }
    });

    clusters[closestCentroidIndex].push(point);
  });

  return clusters;
}

function recalculateCentroids(clusters) {
  return clusters.map(
    (cluster) => cluster.reduce((sum, value) => sum + value, 0) / cluster.length
  );
}

function isEqual(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function calculateWCSS(cluster) {
  let centroid =
    cluster.reduce((sum, value) => sum + value, 0) / cluster.length;
  return cluster.reduce((sum, point) => sum + Math.pow(point - centroid, 2), 0);
}

function elbowMethod(data, maxK) {
  let wcssValues = [];
  let reductions = [];

  for (let k = 1; k <= maxK; k++) {
    let clusters = kmeans(data, k);
    let currentWCSS = clusters.reduce(
      (sum, cluster) => sum + calculateWCSS(cluster),
      0
    );
    wcssValues.push(currentWCSS);
  }

  for (let i = 1; i < wcssValues.length; i++) {
    reductions.push(wcssValues[i - 1] - wcssValues[i]);
  }

  let biggestDrop = Math.max(...reductions);
  let optimalK = reductions.indexOf(biggestDrop) + 2; // +2 because index starts from 0 and we're looking from k=2 onwards

  return optimalK;
}

function normalize(data) {
  const max = Math.max(...data);
  const min = Math.min(...data);
  return data.map((point) => (point - min) / (max - min));
}

function denormalize(data, originalData) {
  const max = Math.max(...originalData);
  const min = Math.min(...originalData);
  return data.map((normalizedPoint) => normalizedPoint * (max - min) + min);
}
function optimalKMeansCluster(data) {
  const normalizedData = normalize(data);
  const k = elbowMethod(normalizedData, 20); // assuming maxK as 10 for simplicity
  const normalizedClusters = kmeans(normalizedData, k);

  return normalizedClusters.map((cluster) =>
    cluster.map((normalizedPoint) => denormalize([normalizedPoint], data)[0])
  );
}

export default optimalKMeansCluster;
