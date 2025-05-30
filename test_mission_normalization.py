#!/usr/bin/env python3
"""
Test Mission Normalization System
Verify the unified parser correctly normalizes all mission types from actual Deadside.log
"""

import asyncio
import re
from pathlib import Path
from bot.parsers.unified_log_parser import UnifiedLogParser

class MockBot:
    """Mock bot for testing"""
    def __init__(self):
        pass

async def test_mission_normalization():
    """Test mission normalization with actual log data"""
    print("🧪 Testing Mission Normalization System")
    print("=" * 50)
    
    # Initialize parser
    parser = UnifiedLogParser(MockBot())
    
    # Read actual log file
    log_path = Path('./attached_assets/Deadside.log')
    if not log_path.exists():
        print("❌ Deadside.log not found")
        return
    
    print(f"📄 Reading log file: {log_path}")
    
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Extract all mission events
    mission_pattern = re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+)', re.IGNORECASE)
    missions = set(mission_pattern.findall(content))
    
    print(f"📊 Found {len(missions)} unique mission types")
    print()
    
    # Test normalization for each mission
    normalized_results = {}
    for mission_id in sorted(missions):
        normalized_name = parser.normalize_mission_name(mission_id)
        mission_level = parser.get_mission_level(mission_id)
        normalized_results[mission_id] = {
            'normalized': normalized_name,
            'level': mission_level
        }
    
    # Display results
    print("🎯 MISSION NORMALIZATION RESULTS:")
    print("-" * 80)
    print(f"{'Original Mission ID':<35} {'Normalized Name':<30} {'Level'}")
    print("-" * 80)
    
    for mission_id, data in normalized_results.items():
        level_stars = "⭐" * data['level']
        print(f"{mission_id:<35} {data['normalized']:<30} {level_stars}")
    
    print("-" * 80)
    
    # Verify no "Unknown" missions
    unknown_count = sum(1 for data in normalized_results.values() 
                       if "Unknown" in data['normalized'])
    
    print(f"\n✅ Mission Types Processed: {len(missions)}")
    print(f"✅ All Missions Normalized: {unknown_count == 0}")
    print(f"✅ No 'Unknown' Labels: {unknown_count == 0}")
    
    if unknown_count > 0:
        print(f"❌ Found {unknown_count} missions with 'Unknown' labels")
        for mission_id, data in normalized_results.items():
            if "Unknown" in data['normalized']:
                print(f"  - {mission_id}: {data['normalized']}")
    
    # Test embed creation for sample missions
    print(f"\n🎨 Testing Embed Creation:")
    print("-" * 50)
    
    sample_missions = list(missions)[:3]
    for mission_id in sample_missions:
        embed = await parser.process_mission_event("test_guild", mission_id, "READY")
        if embed:
            print(f"✅ {mission_id}: Embed created successfully")
            print(f"   Title: {embed.title}")
            print(f"   Description: {embed.description}")
        else:
            print(f"❌ {mission_id}: Failed to create embed")
        print()

if __name__ == "__main__":
    asyncio.run(test_mission_normalization())