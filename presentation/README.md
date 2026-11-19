# YouTube Pulse — Technical Presentation (16:9)

This directory contains a standalone, static 16:9 HTML/CSS/JavaScript technical presentation for the **YouTube Pulse** (`Avighn0611/BDA-youtube`) Big Data Analytics project.

## Files

```text
presentation/
├── index.html                        # Complete 12-slide presentation with inline SVG diagrams and charts
├── styles.css                        # 1920x1080 widescreen design system and @media print PDF stylesheet
├── script.js                         # Viewport scaling, keyboard/touch navigation, fullscreen, and slide grid
├── YouTube-Pulse-Presentation.pdf    # Pre-exported 12-page 16:9 landscape PDF version of the deck
├── assets/
│   └── dashboard.png                 # Actual Streamlit dashboard screenshot copied from docs/dashboard.png
└── README.md                         # This usage guide
```

## How to Open and Present

No backend server or build step is required.

1. Open `presentation/index.html` directly in Google Chrome, Microsoft Edge, Firefox, or Safari.
2. Press **`F`** (or click **Fullscreen** in the bottom control bar) to enter classroom presentation mode.
3. Navigate slides using:
   - **`→`**, **`↓`**, **`Space`**, or **`PageDown`** — Next slide
   - **`←`**, **`↑`**, or **`PageUp`** — Previous slide
   - **`Home`** / **`End`** — First / last slide
   - **`G`** or **`O`** — Open the 12-slide overview grid to jump to any slide
   - Direct URL hash links: `#slide-1` through `#slide-12`

## Exporting to 16:9 PDF

1. Click **PDF** in the floating control bar (or press `Ctrl+P` / `Cmd+P`).
2. Choose **Save as PDF**.
3. Enable **Background graphics** and set margins to **None**.
4. The `@media print` stylesheet automatically renders each of the 12 slides as an exact 16:9 landscape page (`1920 × 1080`).

## Slide Overview (13 Slides)

1. **YouTube Pulse - Overview** - Project purpose, core cumulative-counter problem, verified KPIs, and 3-layer stack preview.
2. **The Data Problem** - Four data quality challenges with repository metrics, worked cumulative-view example, and strict scope boundary.
3. **Technology Stack & Roles** - Layered software stack graphic, four architectural principles, and explicit "Not Used" list.
4. **System Architecture Diagram** - Detailed technical SVG topology of the single-host, two-worker HDFS + Spark + Streamlit system with exact ports.
5. **Repository Modules** - Six-card modular breakdown mapping actual repository files to inputs, actions, and outputs.
6. **Execution & Data Flow** - Nine-stage transformation flowchart with validation and Parquet read-back decision diamonds.
7. **Core Engineering Mechanisms** - Multiline CSV repartitioning, two-stage window SQL, UTC parsing, broadcast join, CSV formula-injection escaping, engagement formula, and Spark executor scheduling.
8. **Verified Results - India Run** - Real India dataset charts comparing top 5 categories by latest observed views vs. median engagement.
9. **Dashboard & Applications** - Uncropped actual Streamlit screenshot, 4-tab UI overview, sidebar filter behavior, and 4 practical applications.
10. **Verification & Strengths** - Six-layer verification matrix and 4 key architectural strengths.
11. **Limitations & Future Scope** - Verified single-host limitations paired with the documented 3-PC LAN topology and future roadmap.
12. **Key Takeaways** - Four core technical takeaways and project-by-the-numbers summary strip.
13. **Conclusion** - Core engineering conclusion, six achieved project objectives, and formal committee closing.
