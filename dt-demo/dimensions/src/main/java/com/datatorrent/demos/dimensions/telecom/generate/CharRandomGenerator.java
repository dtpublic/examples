/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Set;

import com.google.common.collect.Sets;

public class CharRandomGenerator implements Generator<Character> {
  public static final CharRandomGenerator digitCharGenerator = new CharRandomGenerator(CharRange.digits);
  
  private char[] candidates;
  
  public CharRandomGenerator(){}
  
  public CharRandomGenerator(CharRange ... ranges)
  {
    addRanges(ranges);
  }
  
  public void addRanges(CharRange ... ranges)
  {
    if(ranges==null || ranges.length==0)
      return;
    Set<Character> chars = Sets.newHashSet();
   
    for(CharRange range : ranges)
    {
      if(range.from == null || range.to == null)
        continue;   //invalid
      for(Character theChar=range.from; theChar<=range.to; ++theChar)
      {
        chars.add(theChar);
      }
    }
    
    //merge with the old candidate
    if(candidates != null && candidates.length > 0)
    {
      for(char theChar : candidates)
        chars.add(theChar);
    }
    
    candidates = new char[chars.size()];
    int index = 0;
    for(char theChar : chars)
    {
      candidates[index++] = theChar;
    }
  }
  
  @Override
  public Character next()
  {
    return candidates[random.nextInt(candidates.length)];
  }
}
