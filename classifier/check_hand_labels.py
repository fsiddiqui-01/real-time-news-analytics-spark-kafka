"""
Part 2.3b: compare your hand labels with the model's predictions.

Fill in the my_label column of hand_label_sample.csv with one of:
    World, Sports, Business, Sci/Tech, None
("None" means the story fits none of the four classes.)
"""

import argparse
from pathlib import Path

import pandas as pd

VALID = ["World", "Sports", "Business", "Sci/Tech", "None"]


def normalise(label):
    """Accept small spelling differences such as 'sci/tech' or ' none '."""
    text = str(label).strip().lower().replace(" ", "")
    for v in VALID:
        if text == v.lower().replace(" ", ""):
            return v
    if text in ("scitech", "sci-tech", "science", "tech"):
        return "Sci/Tech"
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--labels", required=True,
                        help="hand_label_sample.csv with my_label filled in")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    df = pd.read_csv(args.labels, encoding="utf-8-sig", keep_default_na=False)
    df["my_label"] = df["my_label"].map(normalise)

    bad = df[df["my_label"].isna()]
    if not bad.empty:
        print("These rows have a missing or unknown my_label. "
              f"Use one of: {', '.join(VALID)}")
        print(bad[["id", "text"]].to_string(index=False))
        return

    lines = []

    def say(text=""):
        print(text)
        lines.append(str(text))

    none_rows = df[df.my_label == "None"]
    fits = df[df.my_label != "None"].copy()
    fits["correct"] = fits.my_label == fits.predicted_class

    say(f"Hand-labeled messages: {len(df)}")
    say(f"Fit none of the four classes: {len(none_rows)}")
    say(f"Fit one of the four classes: {len(fits)}")
    if len(fits):
        acc = fits.correct.mean()
        say(f"Model accuracy on those {len(fits)}: "
            f"{fits.correct.sum()}/{len(fits)} = {acc:.1%}")

    say("\nYour label (rows) vs model prediction (columns):")
    table = pd.crosstab(df.my_label, df.predicted_class)
    say(table.to_string())

    say("\nMessages the model got wrong:")
    for _, r in fits[~fits.correct].iterrows():
        say(f"- you: {r.my_label} | model: {r.predicted_class} "
            f"({r.probability:.2f}) | {r.text[:120]}")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "hand_labels_vs_model.csv", index=False,
              encoding="utf-8-sig")
    (out / "hand_label_summary.txt").write_text("\n".join(lines) + "\n",
                                                encoding="utf-8")
    print(f"\nSaved: {out / 'hand_labels_vs_model.csv'}")
    print(f"Saved: {out / 'hand_label_summary.txt'}")


if __name__ == "__main__":
    main()
