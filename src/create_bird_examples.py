"""
Create enhanced few-shot examples from BIRD dataset
"""
import json
import random
from pathlib import Path

def create_enhanced_examples():
    """Create diverse few-shot examples from BIRD training data"""

    # Load BIRD training data
    bird_train_path = Path("/Users/toby/prog/kt/rubicon/dataset/BIRD/train/train.json")
    with open(bird_train_path, 'r') as f:
        bird_data = json.load(f)

    # Select diverse examples with different SQL patterns
    selected_examples = []

    # Categories to ensure diversity
    patterns = {
        'simple_select': [],
        'aggregation': [],
        'join': [],
        'group_by': [],
        'subquery': [],
        'order_limit': [],
        'complex': []
    }

    # Categorize examples
    for item in bird_data:
        sql = item['SQL'].upper()

        if 'JOIN' in sql and 'GROUP BY' in sql:
            patterns['complex'].append(item)
        elif 'SELECT' in sql and 'FROM' in sql and 'WHERE' in sql and 'JOIN' in sql:
            patterns['join'].append(item)
        elif 'GROUP BY' in sql:
            patterns['group_by'].append(item)
        elif 'SELECT' in sql and '(' in sql and ')' in sql and 'FROM' in sql.split('(')[1] if '(' in sql else False:
            patterns['subquery'].append(item)
        elif any(func in sql for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
            patterns['aggregation'].append(item)
        elif 'ORDER BY' in sql or 'LIMIT' in sql:
            patterns['order_limit'].append(item)
        else:
            patterns['simple_select'].append(item)

    # Select diverse examples (2-3 from each category)
    for category, items in patterns.items():
        if items:
            # Select up to 3 examples from each category
            num_to_select = min(3, len(items))
            selected = random.sample(items, num_to_select)
            for item in selected:
                selected_examples.append({
                    'question': item['question'],
                    'sql': item['SQL'],
                    'evidence': item.get('evidence', ''),
                    'db_id': item['db_id']
                })

    # Limit to 20 best examples
    selected_examples = selected_examples[:20]

    # Save to file
    output_path = Path(__file__).parent / 'data' / 'bird_few_shot_examples.json'
    output_path.parent.mkdir(exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(selected_examples, f, indent=2)

    print(f"Created {len(selected_examples)} enhanced few-shot examples from BIRD dataset")
    print(f"Saved to {output_path}")

    # Print statistics
    print("\nExample categories:")
    for category, items in patterns.items():
        print(f"  {category}: {len(items)} examples")

if __name__ == "__main__":
    create_enhanced_examples()